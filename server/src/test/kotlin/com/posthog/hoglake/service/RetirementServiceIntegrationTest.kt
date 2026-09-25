package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.RetirementResult
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicLong

/**
 * The paced retirement loop (#193): drop is O(columns), and THIS is
 * what deletes a dropped table's file rows and queues their objects.
 *
 * Every case here is about a GATE or a BOUND, because that is all this
 * loop is. The gate is the catalog's expiry floor — above it a dropped
 * table's rows are still readable by time travel, so deleting them is a
 * correctness bug, not an optimization — and the bounds are what keep
 * the deletion from being the outage the drop used to be.
 *
 * The clock and the inter-batch sleep are INJECTED. A run budget
 * measured against the wall clock would either make this suite slow or
 * make its assertions timing-dependent, and the 750 ms production pause
 * would add minutes of sleeping to a suite that is asserting SQL.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RetirementServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)

    @AfterAll
    fun tearDown() = db.close()

    /**
     * Rows for the adaptive-batch case. Enough that ONE statement over
     * all of them cannot finish inside a 1 ms `statement_timeout` —
     * which is what the case is about — and small enough to seed and
     * delete in a second.
     */
    private val bulkRows = 30_000

    /**
     * A clock the test drives; nanoseconds, monotone by construction.
     *
     * Two modes, and both exist so that a MUTATION fails an assertion
     * rather than hanging the suite. [steps] plays a scripted sequence
     * (the default, all zeros, is "the budget never spends"); [stepNanos]
     * advances a fixed amount per CALL, which bounds any loop that
     * consults the budget however many times it goes round. A test whose
     * mutation would spin needs the second.
     */
    private class TestClock(
        private val steps: List<Long> = emptyList(),
        private val stepNanos: Long = 0,
    ) {
        private val calls = AtomicLong(0)

        /** Nanoseconds elapsed since the run started, per CALL. */
        fun next(): Long {
            val i = calls.getAndIncrement()
            if (stepNanos > 0) return i * stepNanos
            return if (steps.isEmpty()) 0 else steps[minOf(i.toInt(), steps.size - 1)]
        }
    }

    private fun service(
        batch: Int = 1_000,
        budgetMs: Long = 60_000,
        ceiling: Long = 500_000,
        commitLockTimeoutMs: Long = CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
        // THE DEFAULT CLOCK IS BOUNDED, not frozen. 100 ms of virtual
        // time per budget check against the 60 s default budget stops
        // any loop at ~600 iterations — which every case here needs
        // three orders of magnitude fewer of, and which turns a
        // mutation that would SPIN (a `Stuck` that reports progress, a
        // halving that never halves) into a failed assertion instead of
        // a hung suite. A mutation nobody can re-run is a mutation
        // nobody re-runs.
        clock: TestClock = TestClock(stepNanos = 100_000_000),
        sleeps: MutableList<Long> = mutableListOf(),
    ) = RetirementService(
        jdbi,
        batchSize = batch,
        // The production pause is a duty cycle, not a correctness
        // property; the tests record that it was CALLED rather than
        // spending the time.
        pauseMs = 750,
        runBudgetMs = budgetMs,
        queueCeiling = ceiling,
        commitLockTimeoutMs = commitLockTimeoutMs,
        nanoTime = { clock.next() },
        sleep = { sleeps += it },
    )

    // ---- fixture -----------------------------------------------------------

    private fun catalogId(catalog: String) = catalogs.getCatalog(catalog).catalogId

    /**
     * A catalog with one table holding [files] data files, [dvs] of
     * which carry a deletion vector, plus a second table that is never
     * dropped (so every assertion about what retirement TOUCHED has a
     * neighbour it must not touch).
     */
    private fun seed(
        catalog: String,
        files: Int = 4,
        dvs: Int = 0,
    ): Fixture {
        catalogs.createCatalog(catalog, "s3://bucket/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "doomed", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        catalogs.createTable(catalog, "ns", "keeper", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val dataPaths = (0 until files).map { "s3://bucket/$catalog/doomed/f$it.parquet" }
        val appendSnapshot =
            commits.commit(
                catalog,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "doomed", dataPaths.map { FileRegistration(it, 10, 100) }),
                            TableAppend(
                                "ns",
                                "keeper",
                                listOf(FileRegistration("s3://bucket/$catalog/keeper/k.parquet", 7, 70)),
                            ),
                        ),
                ),
            ).snapshotId
        val fileIds =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f
                    JOIN hog_table_version tv
                      ON tv.catalog_id = f.catalog_id AND tv.table_id = f.table_id
                    WHERE f.catalog_id = :c AND tv.name = 'doomed'
                    ORDER BY f.data_file_id
                    """,
                ).bind("c", catalogId(catalog)).mapTo(Long::class.java).list()
            }
        val dvPaths = mutableListOf<String>()
        var head = appendSnapshot
        for (i in 0 until dvs) {
            val path = "s3://bucket/$catalog/doomed/f$i.dv"
            dvPaths += path
            head =
                commits.commit(
                    catalog,
                    CommitRequest(
                        readSnapshot = head,
                        deletes =
                            listOf(
                                TableDeletes(
                                    "ns",
                                    "doomed",
                                    listOf(DeleteFileRegistration(fileIds[i], path, 2, 16)),
                                ),
                            ),
                    ),
                ).snapshotId
        }
        return Fixture(catalogId(catalog), dataPaths, dvPaths, fileIds)
    }

    private data class Fixture(
        val catalogId: Long,
        val dataPaths: List<String>,
        val dvPaths: List<String>,
        val fileIds: List<Long>,
    )

    /** SUPERSEDE a data file's live DV, leaving the old row historical. */
    private fun supersede(
        catalog: String,
        fixture: Fixture,
        index: Int,
        path: String,
    ) {
        val head = catalogs.getCatalog(catalog).headSnapshotId
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = head,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "doomed",
                            listOf(DeleteFileRegistration(fixture.fileIds[index], path, 5, 40)),
                        ),
                    ),
            ),
        )
    }

    /** Move the catalog's expiry floor to [to], as an expiry sweep would. */
    private fun setFloor(
        catalogId: Long,
        to: Long,
    ) = jdbi.useHandleUnchecked { h ->
        h.createUpdate("UPDATE hog_catalog SET earliest_snapshot_id = :f WHERE catalog_id = :c")
            .bind("f", to).bind("c", catalogId).execute()
    }

    private fun floorOf(catalogId: Long): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                .bind("c", catalogId).mapTo(Long::class.java).one()
        }

    private fun liveFiles(
        catalogId: Long,
        tableName: String = "doomed",
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT count(*) FROM hog_data_file f
                JOIN hog_table_version tv
                  ON tv.catalog_id = f.catalog_id AND tv.table_id = f.table_id
                WHERE f.catalog_id = :c AND tv.name = :n
                """,
            ).bind("c", catalogId).bind("n", tableName).mapTo(Long::class.java).one()
        }

    private fun queued(catalogId: Long): List<Triple<String, String, String>> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT path, file_kind, reason FROM hog_file_removal WHERE catalog_id = :c " +
                    "ORDER BY removal_id",
            ).bind("c", catalogId)
                .map { rs, _ ->
                    Triple(rs.getString("path"), rs.getString("file_kind"), rs.getString("reason"))
                }
                .list()
        }

    // ---- the gate ----------------------------------------------------------

    @Test
    fun `a drop above the expiry floor retires nothing, and the floor is why`() {
        val catalog = "ret-gate"
        val f = seed(catalog)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId

        // THE ASSERTION IS THE FLOOR VALUE, not "nothing happened": a
        // test that only checked the row count would pass just as well
        // against a loop that never ran at all.
        assertThat(floorOf(f.catalogId))
            .describedAs("no expiry has run, so the floor is still the catalog's origin")
            .isLessThan(drop)

        val result = service().runOnce(catalog)
        assertThat(result).isEqualTo(RetirementResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        assertThat(liveFiles(f.catalogId)).isEqualTo(4)
        assertThat(queued(f.catalogId)).isEmpty()
    }

    @Test
    fun `a drop exactly AT the floor is eligible, and one below the floor is not`() {
        val catalog = "ret-gate-boundary"
        val f = seed(catalog)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId

        // ONE BELOW: a read at S = drop - 1 is still legal and still
        // needs these rows.
        setFloor(f.catalogId, drop - 1)
        assertThat(service().runOnce(catalog).rowsRetired).isZero()
        assertThat(catalogs.listFiles(catalog, "ns", "doomed", snapshot = drop - 1))
            .describedAs("time travel below the drop still sees every file")
            .hasSize(4)

        // AT THE FLOOR: the version row's end_snapshot IS the drop, so
        // the table is already invisible at S = drop and the rows are
        // unreachable.
        //
        // MUTATIONS, and there are two because the gate is written
        // twice on purpose. Tightening it — `dropped_snapshot <=
        // earliest` to `<` — reds THIS half, in either place. Widening
        // it — `<= earliest + 1` — reds the half above, but ONLY if
        // both copies are widened together: the candidate query
        // proposes the table and `batch`'s re-read of the floor under
        // the commit lock refuses it, so widening one alone is caught
        // by the other. That is the defence working, and it is why the
        // mutation is named as a pair rather than as a line.
        setFloor(f.catalogId, drop)
        val result = service().runOnce(catalog)
        assertThat(result.rowsRetired).isEqualTo(4)
        assertThat(liveFiles(f.catalogId)).isZero()
    }

    @Test
    fun `a fully drained dropped table stops being a candidate`() {
        // `hog_table` ROWS ARE NEVER DELETED — the identity row is the
        // lineage `replaced_table_id` walks and the uuid a consumer's
        // offset keys on — so a table retirement finished six months
        // ago is still dropped, still under the floor, and still
        // matches every clause of the candidate query but the liveness
        // probe. Two things go wrong without it:
        //
        //  - every run takes the per-catalog COMMIT LOCK once per
        //    historical drop, finds nothing, and calls it `Drained`: a
        //    lock hold per dead table, per run, forever;
        //  - and `ORDER BY table_id LIMIT :limit` is a PREFIX, so once
        //    a catalog has accumulated MAX_TABLES_PER_RUN historical
        //    drops with low table ids, the window is ENTIRELY drained
        //    tables and retirement never reaches live work again. It
        //    would stop permanently while reporting zero and looking
        //    healthy.
        //
        // MUTATION: delete the `EXISTS` from CANDIDATE_SQL and this
        // reds — the drained table comes back as a candidate and the
        // second run takes a lock hold for it.
        val catalog = "ret-drained"
        val f = seed(catalog, files = 2)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        val first = service(batch = 10).runOnce(catalog)
        assertThat(first.tables).isEqualTo(1)
        assertThat(first.rowsRetired).isEqualTo(2)
        assertThat(liveFiles(f.catalogId)).isZero()

        // The table row is STILL THERE, still dropped, still under the
        // floor — the state that would make it a candidate forever.
        val stillDropped =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_table WHERE catalog_id = :c " +
                        "AND dropped_snapshot IS NOT NULL AND dropped_snapshot <= :f",
                ).bind("c", f.catalogId).bind("f", drop).mapTo(Long::class.java).one()
            }
        assertThat(stillDropped)
            .describedAs("the identity row outlives retirement by design; that is the hazard")
            .isEqualTo(1)

        // The second run must find NOTHING — and the counters alone
        // CANNOT SEE the bug, which is why the statements are watched.
        // A drained table that comes back as a candidate takes the
        // commit lock, finds no victim, and reports `Drained`: every
        // counter stays zero and the only trace is the LOCK HOLD. So
        // the run is instrumented and the acquisition is what is
        // asserted.
        val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
        val instrumented = com.posthog.hoglake.Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : org.jdbi.v3.core.statement.SqlLogger {
                override fun logAfterExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                    issued += context.renderedSql
                }
            },
        )
        val second =
            RetirementService(
                instrumented,
                batchSize = 10,
                pauseMs = 0,
                nanoTime = { 0 },
                sleep = {},
            ).runOnce(catalog)
        assertThat(second)
            .describedAs("a drained table is finished, not a candidate that drains to zero again")
            .isEqualTo(RetirementResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        // MUTATION: delete the `EXISTS` from CANDIDATE_SQL and THIS is
        // what reds — the counters above stay all-zero either way.
        assertThat(issued.filter { it.contains("pg_advisory_xact_lock") })
            .describedAs(
                "a run with nothing to do must take no commit lock; it issued:%n%s",
                issued.joinToString("\n---\n"),
            )
            .isEmpty()
    }

    @Test
    fun `a floor that moves backwards between batches stops the run mid-table`() {
        // THE ONLY WAY TO REACH `BatchOutcome.NotEligible`, and the
        // reason it needs a test of its own: the floor is MONOTONE —
        // nothing in the server ever lowers `earliest_snapshot_id` — so
        // a table that `candidates` found eligible is still eligible
        // when `batch` re-reads the floor, and the branch is dead code
        // against anything the server itself produces.
        //
        // It is not dead against what an OPERATOR produces: a repair, a
        // restore from a backup taken before the floor moved, a future
        // sweep that learns to move it back. The cost of being wrong
        // there is deleting rows a legal time-travel read still needs,
        // which is unrecoverable, and the re-read costs one bigint
        // under a lock the transaction already holds.
        //
        // The floor is lowered from the INTER-BATCH PAUSE HOOK, which
        // runs between batch transactions and inside none of them —
        // the only place a test can move it without either racing the
        // batch or deadlocking on the commit lock the batch holds.
        //
        // MUTATION: delete the floor re-read and its `if` from
        // `batch()` and this reds — all four rows go, and the
        // time-travel read below loses its files.
        val catalog = "ret-floor-retreat"
        val f = seed(catalog, files = 4)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        val lowered = java.util.concurrent.atomic.AtomicBoolean(false)
        val clock = TestClock(stepNanos = 100_000_000)
        val svc =
            RetirementService(
                jdbi,
                batchSize = 2,
                pauseMs = 1,
                runBudgetMs = 60_000,
                queueCeiling = 500_000,
                commitLockTimeoutMs = CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
                nanoTime = { clock.next() },
                sleep = {
                    if (lowered.compareAndSet(false, true)) setFloor(f.catalogId, drop - 1)
                },
            )

        val result = svc.runOnce(catalog)
        assertThat(lowered.get()).describedAs("the hook must have run, or this asserts nothing").isTrue()
        assertThat(result.rowsRetired)
            .describedAs("the first batch committed; the second must refuse on the re-read")
            .isEqualTo(2)
        assertThat(result.batches).isEqualTo(1)
        assertThat(liveFiles(f.catalogId)).isEqualTo(2)
        // And the rows that survived are readable again, which is the
        // whole point of refusing: at S = drop - 1 the table is live.
        assertThat(catalogs.listFiles(catalog, "ns", "doomed", snapshot = drop - 1))
            .describedAs("a lowered floor makes these rows reachable again")
            .hasSize(2)

        // Put the floor back where the server would have it, and the
        // next run finishes the table: the refusal is a pause, not a
        // poison.
        setFloor(f.catalogId, drop)
        assertThat(service(batch = 2).runOnce(catalog).rowsRetired).isEqualTo(2)
        assertThat(liveFiles(f.catalogId)).isZero()
    }

    @Test
    fun `a catalog with no retention never retires, and that is the design`() {
        val catalog = "ret-no-retention"
        val f = seed(catalog)
        catalogs.dropTable(catalog, "ns", "doomed")
        // Exactly what a production retention-NULL catalog looks like:
        // expiry runs, releases nothing, and never moves the floor.
        ExpiryService(jdbi).runOnce(catalog, batchSize = 1000)
        assertThat(catalogs.getCatalog(catalog).snapshotRetentionSeconds).isNull()
        assertThat(floorOf(f.catalogId)).isZero()

        assertThat(service().runOnce(catalog).rowsRetired).isZero()
        assertThat(liveFiles(f.catalogId)).isEqualTo(4)
    }

    @Test
    fun `a consumer-floored catalog retires only once the offset moves`() {
        val catalog = "ret-consumer"
        val f = seed(catalog)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        // A consumer pinned below the drop: expiry cannot pass it, so
        // the floor stays put and retirement has nothing to do.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = :c",
            ).bind("c", f.catalogId).execute()
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 60, consumer_floor = true " +
                    "WHERE catalog_id = :c",
            ).bind("c", f.catalogId).execute()
            h.createUpdate(
                """
                INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                SELECT :c, 'hedgerow-events', t.table_uuid, 1
                FROM hog_table t WHERE t.catalog_id = :c AND t.dropped_snapshot IS NULL LIMIT 1
                """,
            ).bind("c", f.catalogId).execute()
        }
        val pinned = ExpiryService(jdbi).runOnce(catalog, batchSize = 1000)
        assertThat(pinned.flooredByConsumer).isEqualTo("hedgerow-events")
        assertThat(floorOf(f.catalogId)).isLessThan(drop)
        assertThat(service().runOnce(catalog).rowsRetired).isZero()

        // The consumer catches up; the floor passes the drop; the table
        // retires. Nothing about retirement had to know a consumer
        // exists — the floor is the entire interface.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_consumer_offset SET committed_snapshot = :s WHERE catalog_id = :c",
            ).bind("s", catalogs.getCatalog(catalog).headSnapshotId).bind("c", f.catalogId).execute()
        }
        ExpiryService(jdbi).runOnce(catalog, batchSize = 1000)
        assertThat(floorOf(f.catalogId)).isGreaterThanOrEqualTo(drop)
        assertThat(service().runOnce(catalog).rowsRetired).isEqualTo(4)
    }

    // ---- the happy path ----------------------------------------------------

    @Test
    fun `an eligible table retires DVs first, queues every path once, and cascades`() {
        val catalog = "ret-happy"
        val f = seed(catalog, files = 4, dvs = 2)
        // A SUPERSEDED DV: the one bug hunt #16 was about. It is
        // historical (end_snapshot set), so a DV delete restricted to
        // live vectors would leave it for the data-file cascade to take
        // away un-queued, leaking its puffin object forever.
        val supersededPath = "s3://bucket/$catalog/doomed/f0.dv.v2"
        supersede(catalog, f, 0, supersededPath)

        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        // Stats and partition rows that must ride the cascade out.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id,
                                                   value_count, null_count)
                SELECT :c, data_file_id, 1, 10, 0 FROM hog_data_file WHERE catalog_id = :c
                ON CONFLICT DO NOTHING
                """,
            ).bind("c", f.catalogId).execute()
        }
        setFloor(f.catalogId, drop)

        val sleeps = mutableListOf<Long>()
        // Two rows per batch, so the run is several batches and the
        // pause is exercised between them.
        val result = service(batch = 2, sleeps = sleeps).runOnce(catalog, MaintenanceTrigger.LOOP)

        assertThat(result.tables).isEqualTo(1)
        assertThat(result.rowsRetired).isEqualTo(4)
        assertThat(result.dvsRetired).describedAs("two live DVs plus one superseded").isEqualTo(3)
        assertThat(result.pathsQueued).isEqualTo(7)
        assertThat(result.batches).isEqualTo(2)
        assertThat(result.timeouts).isZero()
        assertThat(result.skippedTables).isZero()
        assertThat(result.skippedQueueFull).isZero()
        assertThat(result.skippedLocked).isZero()
        assertThat(result.tablesRemaining).isZero()
        // The pause ran between batches at the configured duty cycle.
        assertThat(sleeps).isNotEmpty().allSatisfy { assertThat(it).isEqualTo(750L) }

        // Every object path queued EXACTLY ONCE, with the reason that
        // has been in the CHECK constraint since V1 and never written.
        val rows = queued(f.catalogId)
        assertThat(rows.map { it.third }.distinct()).containsExactly("table_drop_gc")
        assertThat(rows.filter { it.second == "data" }.map { it.first })
            .containsExactlyInAnyOrderElementsOf(f.dataPaths)
        // MUTATION: add `AND end_snapshot IS NULL` to the DV delete and
        // this reds — the superseded vector's path is missing, because
        // the data-file cascade took its row away without queueing it.
        assertThat(rows.filter { it.second == "delete" }.map { it.first })
            .containsExactlyInAnyOrderElementsOf(f.dvPaths + supersededPath)
        assertThat(rows.map { it.first })
            .describedAs("one queue row per object; a duplicate is a double delete attempt")
            .doesNotHaveDuplicates()

        // Nothing of the table's file state is left, and the cascades
        // took the children with them.
        assertThat(liveFiles(f.catalogId)).isZero()
        assertThat(liveFiles(f.catalogId, "keeper")).describedAs("the neighbour is untouched").isEqualTo(1)
        jdbi.useHandleUnchecked { h ->
            assertThat(
                h.createQuery(
                    "SELECT count(*) FROM hog_file_column_stats " +
                        "WHERE catalog_id = :c AND data_file_id = ANY(:ids)",
                ).bind("c", f.catalogId)
                    .bindArray("ids", Long::class.javaObjectType, f.fileIds)
                    .mapTo(Long::class.java).one(),
            ).describedAs("per-column stats cascade with their data file").isZero()
            assertThat(
                h.createQuery("SELECT count(*) FROM hog_delete_file WHERE catalog_id = :c")
                    .bind("c", f.catalogId).mapTo(Long::class.java).one(),
            ).isZero()
        }

        // The ledger recorded the run, under the task the migration's
        // CHECK now admits.
        val runs = MaintenanceRunStore(jdbi)
        val last =
            jdbi.withHandleUnchecked { h -> runs.lastByTask(h, f.catalogId) }[MaintenanceTask.RETIREMENT]
        assertThat(last).isNotNull
        assertThat(last!!.trigger).isEqualTo(MaintenanceTrigger.LOOP)
        assertThat(last.resultJson).contains("\"rows_retired\": 4")
    }

    @Test
    fun `retirement_eligible_at is stamped on first observation and never moved`() {
        val catalog = "ret-stamp"
        val f = seed(catalog, files = 6)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        fun stamp(): java.time.Instant? =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT retirement_eligible_at FROM hog_table " +
                        "WHERE catalog_id = :c AND dropped_snapshot IS NOT NULL",
                ).bind("c", f.catalogId)
                    .mapTo(java.time.OffsetDateTime::class.java).findOne().orElse(null)?.toInstant()
            }
        assertThat(stamp()).describedAs("nothing stamps it before a sweep sees the table").isNull()

        // Two batches' worth, one row at a time, so the table survives
        // the first run and is re-observed by the second.
        val first = service(batch = 2, budgetMs = 60_000).runOnce(catalog)
        val firstStamp = stamp()
        assertThat(firstStamp).isNotNull
        assertThat(first.rowsRetired).isEqualTo(6)

        service(batch = 2).runOnce(catalog)
        // MUTATION: drop `AND retirement_eligible_at IS NULL` from the
        // stamp UPDATE and this reds. It matters because /verify dates
        // a leak from this instant: a stamp that keeps moving forward
        // means a table can never be past its grace, and the check can
        // never fire.
        assertThat(stamp()).isEqualTo(firstStamp)
    }

    @Test
    fun `a run interrupted by its budget is continued by the next one`() {
        val catalog = "ret-resume"
        val f = seed(catalog, files = 6)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        // The clock is spent after the first batch: zero on the first
        // two reads (the table-loop check and the first inner check),
        // then past the budget.
        val clock = TestClock(listOf(0, 0, 0, 61_000_000_000))
        val first = service(batch = 2, budgetMs = 60_000, clock = clock).runOnce(catalog)
        // MUTATION: delete the `budgetSpent` check from the inner loop
        // and this reds — the run drains all six rows in one go, and a
        // 50M-row table would hold a pooled connection and a session
        // lock for hours.
        assertThat(first.rowsRetired).isEqualTo(2)
        assertThat(first.batches).isEqualTo(1)
        assertThat(first.tablesRemaining)
            .describedAs("the run says it stopped short rather than presenting as idle")
            .isEqualTo(1)
        assertThat(liveFiles(f.catalogId)).isEqualTo(4)

        // NO CURSOR IS CARRIED. The next run re-selects "whatever is
        // still live on this dropped table" and finishes it.
        val second = service(batch = 2).runOnce(catalog)
        assertThat(second.rowsRetired).isEqualTo(4)
        assertThat(liveFiles(f.catalogId)).isZero()
    }

    // ---- the bounds --------------------------------------------------------

    @Test
    fun `a backed-up cleanup queue stops the run before it starts`() {
        val catalog = "ret-ceiling"
        val f = seed(catalog)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT :c, 's3://bucket/$catalog/backlog/' || g || '.parquet', 'data', 'snapshot_expiry'
                FROM generate_series(1, 5) g
                """,
            ).bind("c", f.catalogId).execute()
        }

        // MUTATION: delete the ceiling check and this reds. Retirement's
        // output IS cleanup's input, and the drain is the slower of the
        // two: a 3M-row table unpaced against it is a 3M-row queue
        // (~1.9 GB of ledger) the drain cannot absorb.
        val result = service(ceiling = 4).runOnce(catalog)
        assertThat(result.skippedQueueFull).isEqualTo(1)
        assertThat(result.rowsRetired).isZero()
        assertThat(liveFiles(f.catalogId)).isEqualTo(4)

        // A SKIPPED RUN STILL STAMPS ELIGIBILITY, and that is what
        // keeps the skip out of SILENCE. `/verify`'s orphans arm dates
        // a leak from `retirement_eligible_at`, so a catalog whose
        // cleanup queue never drains — the state an operator most needs
        // told about — would otherwise never be stamped, the arm could
        // never fire, and retirement would wedge with every counter at
        // zero and every check green.
        //
        // MUTATION: move `stampEligible` back below the ceiling check
        // and this reds.
        val stamped =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_table WHERE catalog_id = :c " +
                        "AND retirement_eligible_at IS NOT NULL",
                ).bind("c", f.catalogId).mapTo(Long::class.java).one()
            }
        assertThat(stamped)
            .describedAs("a run the ceiling stopped must still record that the table became retirable")
            .isEqualTo(1)
        // ...and it says so in the result, rather than presenting as a
        // run that found nothing to do.
        assertThat(result.tablesRemaining)
            .describedAs("the eligible table is remaining work, not absence of work")
            .isEqualTo(1)

        // /verify AGREES, once the table is past the grace: the arm
        // that could never fire now does. This is the end-to-end form
        // of the same assertion, through the check an operator reads.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_table SET retirement_eligible_at = now() - interval '1 hour'
                WHERE catalog_id = :c AND dropped_snapshot IS NOT NULL
                """,
            ).bind("c", f.catalogId).execute()
        }
        val orphans =
            VerifyService(jdbi, retirementOrphanGraceSeconds = 1, retirementIntervalMs = 0)
                .runOnce(catalog)
                .checks
                .single { it.check == "orphans" }
        assertThat(orphans.status)
            .describedAs("a wedged retirement must be visible in /verify, not only in the logs")
            .isEqualTo("fail")
        assertThat(orphans.samples).anySatisfy {
            assertThat(it).contains("retirement is not draining it")
        }

        // One more than the queue holds: the run proceeds.
        assertThat(service(ceiling = 5).runOnce(catalog).rowsRetired).isEqualTo(4)
    }

    @Test
    fun `a second maintainer skips a catalog rather than queueing behind it`() {
        val catalog = "ret-single-flight"
        val f = seed(catalog)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        jdbi.open().use { holder ->
            assertThat(Locks.tryAcquireCatalogRetirementLock(holder, f.catalogId)).isTrue()
            // MUTATION: swap `pg_try_advisory_lock` for
            // `pg_advisory_lock` and this test HANGS instead of
            // passing, which is the failure it exists to prevent: W
            // maintainers queued on one catalog tax every foreground
            // commit by (W - 0.5) x hold.
            val blocked = service().runOnce(catalog)
            assertThat(blocked.skippedLocked).isEqualTo(1)
            assertThat(blocked.rowsRetired).isZero()
            assertThat(liveFiles(f.catalogId)).isEqualTo(4)
            Locks.releaseCatalogRetirementLock(holder, f.catalogId)
        }

        // The lock is released with the run, so the next one proceeds.
        assertThat(service().runOnce(catalog).rowsRetired).isEqualTo(4)
    }

    @Test
    fun `a batch too big for its table is halved rather than failing the run`() {
        val catalog = "ret-adaptive"
        val f = seed(catalog, files = 1)
        // A manifest big enough that a whole-batch DELETE cannot finish
        // inside a 1 ms bound, seeded by SQL rather than by commits: the
        // point is the SIZE of one statement's work, and thirty thousand
        // round trips through the commit service would be a minute of
        // suite time asserting nothing.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 900000 + g, (SELECT table_id FROM hog_table_version
                                         WHERE catalog_id = :c AND name = 'doomed'
                                           AND end_snapshot IS NULL),
                       1, 's3://bucket/$catalog/doomed/bulk-' || g || '.parquet', 1, 1, g
                FROM generate_series(1, :n) g
                """,
            ).bind("c", f.catalogId).bind("n", bulkRows).execute()
        }
        val before = liveFiles(f.catalogId)
        assertThat(before).isEqualTo(bulkRows + 1L)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        // A 10 ms statement bound: admission 80 ms / 2 = 40, divided
        // by BOUNDED_STATEMENTS_PER_BATCH. (It was written as 20 before
        // the bound started being divided by the statement count, which
        // silently made it 2 ms — near the noise floor, and the test
        // went intermittent.) A
        // 30,000-row DELETE with its FK cascades and its queue INSERT
        // is ~600 ms at the per-row cost `RetirementCostIntegrationTest`
        // measures, so it cannot finish inside it; halving reaches a
        // size that can after about six steps. The bound is chosen to
        // be comfortably above a SMALL batch's cost and comfortably
        // below a large one's, so the case tests the adaptation rather
        // than the machine's scheduler — a 1 ms bound is under the cost
        // of a ONE-ROW batch too, and would make "it never fits" the
        // outcome on a slow run.
        //
        // The rollback is whole: "too big" is a fact about the table's
        // cascade fan-out, not about the config.
        //
        // MUTATION: remove `n = halved` and this reds — every batch
        // keeps timing out at the original size, the run retires
        // nothing, and the only thing that ends it is the run budget.
        // The clock advances 10 ms per budget check and the budget is
        // 2 s, so the run is bounded at ~200 iterations WHATEVER the
        // loop does. That is deliberate: without it, the mutation named
        // below does not fail an assertion, it spins — and a mutation
        // that hangs the suite is a mutation nobody re-runs.
        val result =
            service(
                batch = bulkRows,
                budgetMs = 2_000,
                commitLockTimeoutMs = 80,
                clock = TestClock(stepNanos = 10_000_000),
            ).runOnce(catalog)
        println(
            "[#193] adaptive batch: started at $bulkRows rows against a 10ms statement bound, " +
                "took ${result.timeouts} rolled-back batches to find a size that fits, then " +
                "${result.batches} batches to retire ${result.rowsRetired} rows",
        )
        assertThat(result.timeouts)
            .describedAs("the oversized batches are counted, not swallowed")
            .isGreaterThan(0)
        assertThat(liveFiles(f.catalogId))
            .describedAs("halving made progress: fewer rows than it started with")
            .isLessThan(before)
        // Whatever was retired was retired PROPERLY — a rolled-back
        // batch leaves no half-queued path behind, so the queue holds
        // exactly what left the manifest.
        assertThat(queued(f.catalogId).size.toLong())
            .isEqualTo(before - liveFiles(f.catalogId))
    }

    @Test
    fun `a timed-out batch pauses too, and the size it settled on survives the run`() {
        // TWO PROPERTIES OF THE SAME LOOP, and both are about the lock
        // rather than about throughput.
        //
        // A rolled-back batch still HELD the commit lock for its whole
        // statement bound before it was cancelled, so retrying
        // immediately turns a halving sequence into a near-continuous
        // hold — the duty cycle is about the lock, and the lock does
        // not care whether the transaction committed.
        //
        // And `n` used to reset to the configured batch at the top of
        // every table of every run, so a wide table paid its whole
        // halving sequence — each step a rolled-back hold — EVERY RUN.
        // The size the halving settled on is remembered per table for
        // the life of the process.
        val catalog = "ret-remember"
        val f = seed(catalog, files = 1)
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 900000 + g, (SELECT table_id FROM hog_table_version
                                         WHERE catalog_id = :c AND name = 'doomed'
                                           AND end_snapshot IS NULL),
                       1, 's3://bucket/$catalog/doomed/bulk-' || g || '.parquet', 1, 1, g
                FROM generate_series(1, :n) g
                """,
            ).bind("c", f.catalogId).bind("n", bulkRows).execute()
        }
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        val sleeps = mutableListOf<Long>()
        // A budget of 500 ms at 10 ms of virtual time per check is ~50
        // iterations: enough for the halving search plus real progress,
        // and NOT enough to drain the table — which the second half of
        // this case needs, and which the assertion below states so a
        // future change that drains it all fails loudly instead of
        // confusingly.
        val svc =
            service(
                batch = bulkRows,
                budgetMs = 500,
                commitLockTimeoutMs = 80,
                clock = TestClock(stepNanos = 10_000_000),
                sleeps = sleeps,
            )
        val first = svc.runOnce(catalog)
        assertThat(first.timeouts).isGreaterThan(0)
        // MUTATION: remove the `sleep(pauseMs)` from the Timeout branch
        // and this reds — the pause count drops to the committed
        // batches alone, and the rolled-back holds run back to back.
        assertThat(sleeps.size.toLong())
            .describedAs(
                "every hold pauses, committed or rolled back: %d batches + %d timeouts, %d pauses",
                first.batches,
                first.timeouts,
                sleeps.size,
            )
            .isEqualTo(first.batches + first.timeouts)

        val leftBefore = liveFiles(f.catalogId)
        assertThat(leftBefore)
            .describedAs("the first run must leave work for the second, or there is nothing to assert")
            .isGreaterThan(0)

        // MUTATION: drop `settledBatchSize` and start from `batchSize`
        // again, and this reds with a second round of timeouts.
        val second = svc.runOnce(catalog)
        assertThat(first.timeouts)
            .describedAs("the first run must really have searched, or there is nothing to remember")
            .isGreaterThan(1)
        // FEWER, not zero. The remembered size is the one that worked
        // last time, and a batch that fits in 10 ms on one run can
        // exceed it on the next when the machine is busy — that is the
        // adaptation doing its job, not a regression. What must not
        // happen is the whole halving sequence being paid again.
        assertThat(second.timeouts)
            .describedAs(
                "the second run starts from the size the first one settled on, so it repeats at " +
                    "most a step of the search rather than all of it (first run: %d timeouts, " +
                    "%d rows left for the second)",
                first.timeouts,
                leftBefore,
            )
            .isLessThan(first.timeouts)
        assertThat(second.rowsRetired).isGreaterThan(0)
    }

    @Test
    fun `a batch that selects rows and deletes none ends the run for that table`() {
        val catalog = "ret-stuck"
        val f = seed(catalog)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)
        // Fault injection: a BEFORE DELETE trigger that swallows the
        // delete. The state is structurally impossible on a healthy
        // catalog — the victim select and the DELETE name the same
        // primary keys — which is exactly why the loop must not spin
        // when it happens.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "CREATE FUNCTION ret_block_delete() RETURNS trigger AS " +
                    "$$ BEGIN RETURN NULL; END $$ LANGUAGE plpgsql",
            )
            h.execute(
                "CREATE TRIGGER ret_block BEFORE DELETE ON hog_data_file " +
                    "FOR EACH ROW EXECUTE FUNCTION ret_block_delete()",
            )
        }
        try {
            // MUTATION: return `Retired(0, 0)` instead of `Stuck` and
            // this reds on the counter while the loop spins until the
            // run budget stops it.
            val result = service(batch = 2).runOnce(catalog)
            assertThat(result.skippedTables).isEqualTo(1)
            assertThat(result.rowsRetired).isZero()
            assertThat(result.batches).isZero()
        } finally {
            jdbi.useHandleUnchecked { h ->
                h.execute("DROP TRIGGER ret_block ON hog_data_file")
                h.execute("DROP FUNCTION ret_block_delete()")
            }
        }
    }

    @Test
    fun `every statement of a batch runs under the batch's own bound, starting with the floor read`() {
        // THE ARITHMETIC IS NOT THE CLAIM. `callBoundMs x
        // BOUNDED_STATEMENTS_PER_BATCH <= admission / 2` is satisfied by
        // any consistent pair of numbers, including a pair that leaves
        // the floor read running under the SESSION's 60 s bound — which
        // is exactly what the code did before: `set_config` sat AFTER
        // the read, so one statement inside a transaction already
        // holding the commit lock could hold it for twice the admission
        // window on its own.
        //
        // So the ORDER is what is asserted, from the statements a real
        // batch issues. MUTATION: move `set_config('statement_timeout')`
        // back below the floor read and this reds; the arithmetic case
        // below does not.
        val catalog = "ret-bound-order"
        val f = seed(catalog, files = 2)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
        val instrumented = com.posthog.hoglake.Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : org.jdbi.v3.core.statement.SqlLogger {
                override fun logAfterExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                    issued += context.renderedSql
                }
            },
        )
        RetirementService(instrumented, batchSize = 1, pauseMs = 0, nanoTime = { 0 }, sleep = {}).runOnce(catalog)

        val lock = issued.indexOfFirst { it.contains("pg_advisory_xact_lock") }
        val bound = issued.indexOfFirst { it.contains("set_config('statement_timeout'") }
        val floorRead = issued.indexOfFirst { it.contains("SELECT earliest_snapshot_id FROM hog_catalog") }
        assertThat(listOf(lock, bound, floorRead))
            .describedAs("all three statements must appear:%n%s", issued.joinToString("\n---\n"))
            .allSatisfy { assertThat(it).isGreaterThanOrEqualTo(0) }
        assertThat(bound)
            .describedAs(
                "the bound goes on immediately after the lock and BEFORE anything it has to " +
                    "bound; statements were:%n%s",
                issued.joinToString("\n---\n"),
            )
            .isGreaterThan(lock)
            .isLessThan(floorRead)
    }

    @Test
    fun `the statement bound is derived from the session and the admission window`() {
        // Not a number somebody liked: min(session statement_timeout / 4,
        // commit admission / 2), both read from their own sources. A
        // zero admission bound means "unbounded wait", never "no
        // statement bound" — the arm drops out of the minimum instead of
        // collapsing it.
        val session = com.posthog.hoglake.Database.SESSION_INIT_SQL_STATEMENT_TIMEOUT.toMillis()
        val statements = RetirementService.BOUNDED_STATEMENTS_PER_BATCH
        assertThat(RetirementService(jdbi, commitLockTimeoutMs = 30_000).callBoundMs)
            .isEqualTo(minOf(session / 4, 30_000 / 2) / statements)
        assertThat(RetirementService(jdbi, commitLockTimeoutMs = 6_000).callBoundMs)
            .describedAs("a tightened admission window tightens the batch with it")
            .isEqualTo(3_000L / statements)
        assertThat(RetirementService(jdbi, commitLockTimeoutMs = 0).callBoundMs)
            .describedAs("no admission bound still leaves a statement bound")
            .isEqualTo(session / 4 / statements)
        assertThat(RetirementService(jdbi, commitLockTimeoutMs = 1).callBoundMs)
            .describedAs("never 0, which Postgres reads as UNLIMITED")
            .isEqualTo(1)

        // THE WHOLE HOLD, which is the thing the admission bound is
        // about. `statement_timeout` applies per STATEMENT, and a batch
        // runs BOUNDED_STATEMENTS_PER_BATCH of them between taking the
        // lock and committing — so a bound of `admission / 2` per
        // statement would have been a hold of 2x admission. MUTATION:
        // drop the division and this reds.
        val admission = com.posthog.hoglake.commit.CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS
        val wholeHold = RetirementService(jdbi, commitLockTimeoutMs = admission).callBoundMs * statements
        assertThat(wholeHold)
            .describedAs("every statement of a batch, summed, must fit inside half the admission window")
            .isLessThanOrEqualTo(admission / 2)
    }
}
