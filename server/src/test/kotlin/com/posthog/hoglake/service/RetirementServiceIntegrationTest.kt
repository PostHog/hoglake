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
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.RetirementGauges
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.testing.PgTestSupport
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.useTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
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
     * `RetirementGauges` is a JVM-GLOBAL object, so a case that leaves a
     * streak behind leaks it into every later case of every later class
     * in the same fork — and the streak cases assert an EXACT row set.
     * Clearing per case costs nothing and is what keeps them from
     * depending on order. (The service instances are per case already;
     * the gauge is the only shared state in this file.)
     */
    @AfterEach
    fun clearGauges() = RetirementGauges.clear()

    /**
     * Rows for the case that drives the real statement bound. Enough
     * that ONE statement over all of them cannot finish inside the 10 ms
     * bound that case configures — which is the whole point of it — and
     * small enough to seed and delete in a second.
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
        // Production's 1,000, lowered only by the case that needs a FULL
        // candidate page to exist.
        maxTablesPerRun: Int = RetirementService.MAX_TABLES_PER_RUN,
        // THE DEFAULT CLOCK IS BOUNDED, not frozen. 100 ms of virtual
        // time per budget check against the 60 s default budget stops
        // any loop at ~600 iterations — which every case here needs
        // three orders of magnitude fewer of, and which turns a
        // mutation that would SPIN (a `Stuck` that reports progress, a
        // Timeout arm that retries the same cold batch instead of
        // leaving the table) into a failed assertion instead of a hung
        // suite. A mutation nobody can re-run is a mutation nobody
        // re-runs.
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
        maxTablesPerRun = maxTablesPerRun,
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

    private fun dvCount(catalogId: Long): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT count(*) FROM hog_delete_file WHERE catalog_id = :c")
                .bind("c", catalogId).mapTo(Long::class.java).one()
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

    /**
     * [n] extra live data files on the dropped table, seeded by SQL
     * rather than by commits: a case about batch boundaries needs more
     * rows than one batch can take, and thousands of round trips
     * through the commit service would be suite time asserting nothing.
     */
    private fun bulkFiles(
        catalog: String,
        catalogId: Long,
        n: Int,
        table: String = "doomed",
        idBase: Int = 900_000,
    ) = jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                       path, record_count, file_size_bytes, row_id_start)
            SELECT :c, :idBase + g, (SELECT table_id FROM hog_table_version
                                      WHERE catalog_id = :c AND name = :t
                                        AND end_snapshot IS NULL),
                   1, 's3://bucket/$catalog/' || :t || '/bulk-' || g || '.parquet', 1, 1,
                   :idBase + g
            FROM generate_series(1, :n) g
            """,
        ).bind("c", catalogId).bind("n", n).bind("t", table).bind("idBase", idBase).execute()
    }

    /**
     * A catalog whose `doomed` table is dropped, eligible, and carries
     * [rows] live file rows. [alsoDropKeeper] drops the neighbour too,
     * so a case can watch the run MOVE ON from one table to the next.
     */
    private fun droppedWithRows(
        catalog: String,
        rows: Int,
        alsoDropKeeper: Boolean = false,
    ): Fixture {
        val f = seed(catalog, files = 1)
        bulkFiles(catalog, f.catalogId, rows - 1)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        val last = if (alsoDropKeeper) catalogs.dropTable(catalog, "ns", "keeper").snapshotId else drop
        setFloor(f.catalogId, last)
        return f
    }

    /**
     * One more dropped, eligible table, with a higher `table_id` than
     * everything created before it — so a candidate page can be
     * genuinely TRUNCATED (the eligible set continuing past the prefix)
     * rather than merely reaching its limit exactly.
     */
    private fun extraDroppedTable(
        catalog: String,
        catalogId: Long,
        name: String,
        idBase: Int,
    ): Long {
        catalogs.createTable(catalog, "ns", name, listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val tableId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT table_id FROM hog_table_version WHERE catalog_id = :c AND name = :n",
                ).bind("c", catalogId).bind("n", name).mapTo(Long::class.java).first()
            }
        bulkFiles(catalog, catalogId, 1, table = name, idBase = idBase)
        val drop = catalogs.dropTable(catalog, "ns", name).snapshotId
        setFloor(catalogId, drop)
        return tableId
    }

    /** The neighbour's id, for the cases that depend on candidate ORDER. */
    private fun keeperTableId(catalogId: Long): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT table_id FROM hog_table_version WHERE catalog_id = :c AND name = 'keeper'",
            ).bind("c", catalogId).mapTo(Long::class.java).first()
        }

    /** The dropped table's id — it outlives the drop, as hog_table rows do. */
    private fun doomedTableId(catalogId: Long): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT table_id FROM hog_table_version WHERE catalog_id = :c AND name = 'doomed'",
            ).bind("c", catalogId).mapTo(Long::class.java).first()
        }

    /**
     * The published consecutive-timeout streak for a catalog's tables,
     * read off the registry the way an alert would — absent is a real
     * answer and must be distinguishable from zero, so this returns a
     * MAP rather than a number.
     */
    private fun streaks(
        registry: PrometheusMeterRegistry,
        catalog: String,
    ): Map<String, Double> =
        registry.find("hoglake_retirement_consecutive_timeouts").tag("catalog", catalog).gauges()
            .associate { it.id.getTag("table")!! to it.value() }

    /**
     * Cancel the given batch ATTEMPTS of one catalog's retirement the
     * way a cold page does — deterministically, with no sleeping, no
     * six-figure fixture and no dependence on the machine's speed.
     *
     * THE ERROR IS THE REAL ONE. `RAISE ... USING ERRCODE = '57014'` is
     * `query_canceled`, the exact SQLSTATE a `statement_timeout`
     * cancellation carries, and `Pg.isQueryCanceled` is what the
     * service reads to tell a cancelled batch from a bug. An injection
     * that raised anything else would exercise the RETHROW path and
     * prove nothing.
     *
     * WHY IT COUNTS ATTEMPTS RATHER THAN ROWS. An AFTER-STATEMENT
     * trigger with a TRANSITION TABLE fires once per INSERT and can
     * still see the rows, so it ignores the DV arm (which queues
     * `delete`-kind paths) and every other case's catalog, and ticks
     * exactly once per batch attempt. The tick is a `nextval`, the one
     * counter in Postgres that a ROLLED-BACK transaction does not take
     * back — which is what makes "cancel attempt 1" mean the first
     * ATTEMPT rather than the first survivor, across runs.
     *
     * The timing-based alternative is `a cancelled batch is counted,
     * the table is left for the next run, and the batch size is NOT
     * changed`'s neighbour below: 30,000 rows against a 10 ms bound.
     * That one proves the bound really fires on real work, which this
     * cannot; this one pins WHICH attempt fails and therefore what the
     * run AFTER it does, which is the whole of #263.
     */
    private fun cancelAttempts(
        tag: String,
        catalogId: Long,
        attempts: List<Int>,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute("CREATE SEQUENCE inject_$tag")
        h.execute(
            """
            CREATE FUNCTION inject_$tag() RETURNS trigger LANGUAGE plpgsql AS $$
            DECLARE k bigint;
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM queued
                                WHERE catalog_id = $catalogId AND file_kind = 'data') THEN
                    RETURN NULL;
                END IF;
                SELECT nextval('inject_$tag') INTO k;
                IF k = ANY (ARRAY[${attempts.joinToString(",")}]) THEN
                    RAISE EXCEPTION 'injected statement cancellation' USING ERRCODE = '57014';
                END IF;
                RETURN NULL;
            END $$
            """,
        )
        h.execute(
            """
            CREATE TRIGGER inject_$tag AFTER INSERT ON hog_file_removal
            REFERENCING NEW TABLE AS queued
            FOR EACH STATEMENT EXECUTE FUNCTION inject_$tag()
            """,
        )
    }

    /**
     * Remove an injection, so no later case's catalog is affected.
     *
     * IF EXISTS ON ALL THREE, and the injection is created INSIDE each
     * case's `try` for the same reason: [cancelAttempts] is three DDL
     * statements, so a failure between the sequence and the trigger
     * would otherwise leave a `finally` that throws on the missing
     * object — and a teardown exception REPLACES the assertion error
     * that sent us there, which is the failure mode that costs an hour
     * reading the wrong stack trace.
     */
    private fun stopCancelling(tag: String) =
        jdbi.useHandleUnchecked { h ->
            h.execute("DROP TRIGGER IF EXISTS inject_$tag ON hog_file_removal")
            h.execute("DROP FUNCTION IF EXISTS inject_$tag()")
            h.execute("DROP SEQUENCE IF EXISTS inject_$tag")
        }

    // ---- the table guard (#264) --------------------------------------------

    @Test
    fun `a lock-held delete that reaches another table's row rolls the batch back`() {
        // The statements are keyed on (catalog_id, data_file_id), the
        // whole identity today; the guard rides on what they RETURN. Run
        // each arm directly with the keeper table's id over a victim
        // list that names the keeper's file beside a doomed one — ids
        // the victim select would never have produced together — and
        // the arm throws, the transaction rolls back, and NOTHING is
        // deleted or queued. Through the service's own `guardedDelete`,
        // so a MUTATION that weakens its check (not a copy of it here)
        // lets the keeper's row go, with its object queued.
        val catalog = "ret-guard"
        val f = seed(catalog, files = 2, dvs = 1)
        val keeper = keeperTableId(f.catalogId)
        val keeperFile =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT data_file_id FROM hog_data_file WHERE catalog_id = :c AND table_id = :t",
                ).bind("c", f.catalogId).bind("t", keeper).mapTo(Long::class.java).one()
            }
        val foreign = listOf(keeperFile, f.fileIds[0])

        val retirement = service()

        fun arm(sql: String) =
            assertThatThrownBy {
                jdbi.useTransactionUnchecked { h ->
                    retirement.guardedDelete(h, sql, f.catalogId, keeper, foreign)
                }
            }.isInstanceOf(IllegalStateException::class.java).hasMessageContaining("of another table")

        // The DV arm: fileIds[0]'s vector belongs to doomed, not keeper.
        arm(RetirementService.DV_DELETE_SQL)
        // The data arm: fileIds[0] itself.
        arm(RetirementService.DATA_DELETE_SQL)

        assertThat(liveFiles(f.catalogId, "keeper")).isEqualTo(1)
        assertThat(liveFiles(f.catalogId, "doomed")).isEqualTo(2)
        assertThat(dvCount(f.catalogId)).isEqualTo(1)
        assertThat(queued(f.catalogId)).isEmpty()
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
        // stamp UPDATE and this reds. It matters because the stamp is
        // the only record of WHEN a table became retirable: one that
        // keeps moving forward means a table can never be past any
        // grace a leak check applies to it. (#261 removed the check that
        // read it; the stamp has to keep accumulating for the
        // replacement to have anything to read.)
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
        // keeps the skip out of SILENCE. A leak check dates itself from
        // `retirement_eligible_at`, so a catalog whose cleanup queue
        // never drains — the state an operator most needs told about —
        // would otherwise never be stamped, nothing could date the leak,
        // and retirement would wedge with every counter at zero.
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

        // There is no second opinion to cross-check against any more:
        // the end-to-end arm here used to drive `/verify`'s orphans
        // check over the same state and assert it went red. #261 removed
        // it, so the stamp above is the whole of the evidence a wedged
        // retirement leaves, and a scrubber that re-reads the stamp is
        // what restores the end-to-end assertion.

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
    fun `a cancelled batch is counted, the table is left for the next run, and the size is NOT changed`() {
        // #263, and the case the whole change exists for. This arm used
        // to HALVE the batch for the table and remember the halved size
        // for the life of the process, which on millpond-prod-us walked
        // a 13.9M-row retirement from 8,000-row batches to 62 in one
        // cold hour and kept it there for a day — and did it again
        // within fifteen minutes of a restart at a configured 1,000.
        //
        // The per-row cost is FLAT in the batch size
        // (`RetirementCostIntegrationTest`), so halving bought a shorter
        // hold by doing proportionally less work, and the cancelled
        // statement had already warmed the pages the retry wants. So:
        // count it, log it, leave the table, and ask for the SAME batch
        // next run.
        //
        // MUTATION: put the halving back (`n = maxOf(1, n / 2)` with or
        // without a remembered size) and this reds on the second run's
        // batch count — 6 batches of 50 instead of 3 of 100.
        val catalog = "ret-no-resize"
        val f = droppedWithRows(catalog, rows = 300)
        try {
            cancelAttempts("noresize", f.catalogId, listOf(1))
            val svc = service(batch = 100)
            val first = svc.runOnce(catalog)
            assertThat(first.timeouts)
                .describedAs("the cancelled batch is counted ONCE: the run does not retry it")
                .isEqualTo(1)
            assertThat(first.batches).isZero()
            assertThat(first.rowsRetired).isZero()
            assertThat(liveFiles(f.catalogId))
                .describedAs("the batch rolled back whole")
                .isEqualTo(300)
            assertThat(first.tablesRemaining)
                .describedAs(
                    "a table left for the next run is not a FINISHED table, so it is remaining " +
                        "work — the reading that keeps a cold table out of the quiet branch",
                )
                .isEqualTo(1)

            // The next run asks for the same rows at the same size, and
            // the pages the cancelled statement warmed are the pages it
            // wants: 300 rows at 100 is three batches, not six.
            val second = svc.runOnce(catalog)
            assertThat(second.batches)
                .describedAs("300 rows in batches of the CONFIGURED 100; a halved size would be 6")
                .isEqualTo(3)
            assertThat(second.timeouts).isZero()
            assertThat(second.rowsRetired).isEqualTo(300)
            assertThat(liveFiles(f.catalogId)).isZero()
            assertThat(queued(f.catalogId)).hasSize(300)
        } finally {
            stopCancelling("noresize")
        }
    }

    @Test
    fun `the statement bound fires on real work, and one cancelled batch ends the table for the run`() {
        val catalog = "ret-real-bound"
        val f = seed(catalog, files = 1)
        // A manifest big enough that a whole-batch DELETE cannot finish
        // inside the bound, seeded by SQL rather than by commits: the
        // point is the SIZE of one statement's work, and thirty thousand
        // round trips through the commit service would be a minute of
        // suite time asserting nothing.
        bulkFiles(catalog, f.catalogId, bulkRows)
        val before = liveFiles(f.catalogId)
        assertThat(before).isEqualTo(bulkRows + 1L)
        val drop = catalogs.dropTable(catalog, "ns", "doomed").snapshotId
        setFloor(f.catalogId, drop)

        // A 10 ms statement bound: admission 80 ms / 2 = 40, divided by
        // BOUNDED_STATEMENTS_PER_BATCH. (It was written as 20 before the
        // bound started being divided by the statement count, which
        // silently made it 2 ms — near the noise floor, and the test
        // went intermittent.) A 30,000-row DELETE with its FK cascades
        // and its queue INSERT is ~600 ms at the per-row cost
        // `RetirementCostIntegrationTest` measures, so it cannot finish
        // inside it.
        //
        // THE INJECTED-CANCELLATION CASES CANNOT PROVE THIS. A trigger
        // raising 57014 proves what the service does with the
        // exception; this proves that the bound fires on real work, on
        // the real statements, with the real rollback — and that the
        // loop neither spins on it nor needs its run budget to stop.
        //
        // MUTATION: remove `done = true` from the Timeout arm and this
        // reds — the table is retried inside the run until the budget
        // (10 ms of virtual time per check against 2 s) stops it, ~200
        // iterations of 10 ms holds, and `timeouts` is in the hundreds.
        val svc =
            service(
                batch = bulkRows,
                budgetMs = 2_000,
                commitLockTimeoutMs = 80,
                clock = TestClock(stepNanos = 10_000_000),
            )
        val result = svc.runOnce(catalog)
        println(
            "[#263] bound fires on real work: a batch of $bulkRows rows against a " +
                "${svc.callBoundMs}ms statement bound was cancelled ${result.timeouts} time(s), " +
                "retired ${result.rowsRetired} rows, and left ${result.tablesRemaining} " +
                "table(s) for the next run",
        )
        assertThat(result.timeouts)
            .describedAs("ONE cancelled batch, then the table is left — no retry, no spin")
            .isEqualTo(1)
        assertThat(result.rowsRetired).isZero()
        assertThat(liveFiles(f.catalogId))
            .describedAs("the rollback is whole: nothing left the manifest")
            .isEqualTo(before)
        assertThat(queued(f.catalogId))
            .describedAs("and nothing was queued — a rolled-back batch leaves no half-queued path")
            .isEmpty()
        assertThat(result.tablesRemaining).isEqualTo(1)
    }

    @Test
    fun `a cancelled batch pays the pause, and the run spends the rest of its budget on the next table`() {
        // TWO PROPERTIES OF THE SAME ARM, and both are about the lock.
        //
        // A rolled-back batch still HELD the commit lock for its whole
        // statement bound before it was cancelled, so moving straight
        // on to the next table's first batch would turn two tables'
        // holds into one near-continuous one — the duty cycle is about
        // the lock, and the lock does not care whether the transaction
        // committed.
        //
        // And a cold table must not cost the catalog its whole run: the
        // run leaves it and retires what it can elsewhere. `keeper` is
        // dropped here too, so there IS an elsewhere.
        val catalog = "ret-moves-on"
        val f = droppedWithRows(catalog, rows = 300, alsoDropKeeper = true)
        // THE NARRATIVE DEPENDS ON CANDIDATE ORDER. `CANDIDATE_SQL` is
        // `ORDER BY t.table_id`, so "the cold table first, then the
        // one it moved on to" is only what this case tests while
        // `doomed` has the lower id — which it does because `seed`
        // creates it first, and which is asserted rather than assumed
        // so a fixture reordering cannot silently invert the case.
        assertThat(doomedTableId(f.catalogId))
            .describedAs("the cold table must be the FIRST candidate for this case to mean anything")
            .isLessThan(keeperTableId(f.catalogId))
        try {
            cancelAttempts("moveson", f.catalogId, listOf(1))
            val sleeps = mutableListOf<Long>()
            val result = service(batch = 100, sleeps = sleeps).runOnce(catalog)
            assertThat(result.timeouts).isEqualTo(1)
            // MUTATION: remove the `sleep(pauseMs)` from the Timeout
            // branch and this reds — the pause count drops to the
            // committed batches alone, and a rolled-back hold runs back
            // to back with the next table's.
            assertThat(sleeps.size.toLong())
                .describedAs(
                    "every hold pauses, committed or rolled back: %d batches + %d timeouts",
                    result.batches,
                    result.timeouts,
                )
                .isEqualTo(result.batches + result.timeouts)
            // `keeper` holds one file and is dropped: the run moved on
            // to it and retired it.
            assertThat(result.rowsRetired).isEqualTo(1)
            assertThat(liveFiles(f.catalogId, "keeper")).isZero()
            assertThat(liveFiles(f.catalogId))
                .describedAs("the cold table is untouched, and left whole")
                .isEqualTo(300)
            assertThat(result.tables)
                .describedAs("one table was retired from; the cold one was not")
                .isEqualTo(1)
            assertThat(result.tablesRemaining)
                .describedAs("the cold table is remaining work even though the run passed it")
                .isEqualTo(1)
        } finally {
            stopCancelling("moveson")
        }
    }

    @Test
    fun `a table that times out run after run publishes a rising streak, and a committed batch retires it`() {
        // THE ONE CASE THAT NEEDS A HUMAN, and the only reason this loop
        // still has a per-table memory of anything. An occasional
        // timeout is a cold batch and means nothing; the SAME table
        // failing run after run is a table whose per-row cascade does
        // not fit the bound at HOGLAKE_RETIREMENT_BATCH, which no retry
        // can fix. `hoglake_retirement_timeouts_total` cannot tell the
        // two apart — it is one number for the catalog — so the gauge
        // is per table, and it RETIRES when the table recovers, which a
        // counter could never do.
        //
        // MUTATION: remove the `RetirementGauges.recovered` call and the
        // third run leaves a series at 2 — an alert nobody can close on
        // a table that is fine. Remove the `timedOut` call and the first
        // two runs publish nothing at all.
        val catalog = "ret-streak"
        val f = droppedWithRows(catalog, rows = 300)
        val tableId = doomedTableId(f.catalogId).toString()
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        RetirementGauges.clear()
        try {
            cancelAttempts("streak", f.catalogId, listOf(1, 2))
            val svc = service(batch = 100)

            assertThat(svc.runOnce(catalog).timeouts).isEqualTo(1)
            assertThat(streaks(registry, catalog))
                .describedAs("one cold run is a published 1, which is deliberately not an alert")
                .isEqualTo(mapOf(tableId to 1.0))

            assertThat(svc.runOnce(catalog).timeouts).isEqualTo(1)
            assertThat(streaks(registry, catalog))
                .describedAs("consecutive, so it rises: this is the series an operator alerts on")
                .isEqualTo(mapOf(tableId to 2.0))

            val third = svc.runOnce(catalog)
            assertThat(third.timeouts).isZero()
            assertThat(third.rowsRetired).isEqualTo(300)
            assertThat(streaks(registry, catalog))
                .describedAs(
                    "a committed batch ends the streak and the SERIES GOES AWAY — absence is " +
                        "the healthy state, which is why this is a MultiGauge and not a counter",
                )
                .isEmpty()

            // And a run with nothing eligible keeps it that way: the
            // candidate set is authoritative, an empty one included.
            assertThat(svc.runOnce(catalog).tables).isZero()
            assertThat(streaks(registry, catalog)).isEmpty()
        } finally {
            Metrics.clear()
            RetirementGauges.clear()
            stopCancelling("streak")
        }
    }

    @Test
    fun `a table that times out and then goes STUCK stops publishing a streak`() {
        // ANY NON-TIMEOUT TERMINAL OUTCOME ENDS THE STREAK, not only a
        // committed batch. Without that, a table that times out twice
        // and then wedges on a swallowed DELETE republishes its 2
        // forever: an alert pointing at HOGLAKE_RETIREMENT_BATCH while
        // the actual fault is `skipped_tables`, which has its own
        // counter and its own remedy.
        //
        // MUTATION: remove the `RetirementGauges.recovered` call from
        // the Stuck arm and this reds — the series stays at 1 with
        // nothing left that can clear it.
        val catalog = "ret-streak-stuck"
        val f = droppedWithRows(catalog, rows = 40)
        val tableId = doomedTableId(f.catalogId).toString()
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        RetirementGauges.clear()
        try {
            cancelAttempts("stuckstreak", f.catalogId, listOf(1))
            val svc = service(batch = 20)
            assertThat(svc.runOnce(catalog).timeouts).isEqualTo(1)
            assertThat(streaks(registry, catalog)).isEqualTo(mapOf(tableId to 1.0))

            // The Stuck arm's fault injection, as its own case uses it:
            // a BEFORE DELETE trigger that swallows the delete, so the
            // victim select returns rows and the DELETE removes none.
            jdbi.useHandleUnchecked { h ->
                h.execute(
                    "CREATE FUNCTION swallow_delete() RETURNS trigger AS " +
                        "$$ BEGIN RETURN NULL; END $$ LANGUAGE plpgsql",
                )
                h.execute(
                    "CREATE TRIGGER swallow_delete BEFORE DELETE ON hog_data_file " +
                        "FOR EACH ROW EXECUTE FUNCTION swallow_delete()",
                )
            }
            try {
                val stuck = svc.runOnce(catalog)
                assertThat(stuck.skippedTables)
                    .describedAs("the run reports the real fault, which is not a timeout")
                    .isEqualTo(1)
                assertThat(stuck.timeouts).isZero()
                assertThat(streaks(registry, catalog))
                    .describedAs(
                        "the table is no longer failing on its statement bound, so the series " +
                            "that claims it is must go",
                    )
                    .isEmpty()
            } finally {
                jdbi.useHandleUnchecked { h ->
                    h.execute("DROP TRIGGER IF EXISTS swallow_delete ON hog_data_file")
                    h.execute("DROP FUNCTION IF EXISTS swallow_delete()")
                }
            }
        } finally {
            Metrics.clear()
            RetirementGauges.clear()
            stopCancelling("stuckstreak")
        }
    }

    @Test
    fun `a table that stops being eligible loses its timeout series without ever committing a batch`() {
        // THE LEAK A COUNTER CANNOT AVOID AND A GAUGE STILL CAN, so it
        // is pinned: the streak ends on a committed batch, but a table
        // can also simply stop being eligible — drained by an operator's
        // repair, or by anything else that takes its live file rows
        // away. Nothing then commits a batch for it, so without the
        // candidate-set retain its row would be republished at its last
        // value forever: an alert about a table that no longer exists,
        // which is `ExpiryGauges.retain`'s unclosable alert.
        //
        // MUTATION: remove the `RetirementGauges.retain` call and this
        // reds — the series stays at 1 with nothing left to clear it.
        val catalog = "ret-streak-retain"
        val f = droppedWithRows(catalog, rows = 20)
        val tableId = doomedTableId(f.catalogId).toString()
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        RetirementGauges.clear()
        try {
            cancelAttempts("retain", f.catalogId, listOf(1))
            val svc = service(batch = 10)
            assertThat(svc.runOnce(catalog).timeouts).isEqualTo(1)
            assertThat(streaks(registry, catalog)).isEqualTo(mapOf(tableId to 1.0))

            // The rows go away without retirement doing it, which is
            // what makes the table stop being a candidate.
            jdbi.useHandleUnchecked { h ->
                h.createUpdate("DELETE FROM hog_data_file WHERE catalog_id = :c")
                    .bind("c", f.catalogId).execute()
            }
            val idle = svc.runOnce(catalog)
            assertThat(idle.tables).isZero()
            assertThat(streaks(registry, catalog))
                .describedAs("the candidate set is authoritative, and an EMPTY one is an answer")
                .isEmpty()
        } finally {
            Metrics.clear()
            RetirementGauges.clear()
            stopCancelling("retain")
        }
    }

    @Test
    fun `a full candidate page still prunes within its own prefix, and keeps what is beyond it`() {
        // COPILOT'S FINDING ON #282, and the one that needed a real
        // regression test. The first version of this skipped pruning
        // ENTIRELY on a full candidate page, on the correct observation
        // that `ORDER BY table_id LIMIT n` says nothing about the ids
        // past its last one. But it also says EVERYTHING about the ids
        // below: a table the page did not return, below its ceiling, was
        // offered to that ORDER BY and refused — it is gone. Skipping
        // the prune left such a table's streak published with nothing
        // that could ever clear it, for as long as the catalog's pages
        // stayed full, which on the mass drop this guard was written for
        // is indefinitely.
        //
        // The page is made full by lowering the bound rather than by
        // seeding a thousand dropped tables: the property under test is
        // `candidates.size >= maxTablesPerRun`, and 2 reaches it.
        //
        // MUTATION: go back to `if (candidates.size < maxTablesPerRun)`
        // around the retain and this reds on the drained LOW table,
        // whose series survives its own table. Drop the `prefixCeiling`
        // term and it reds on the HIGH one instead, retired while it is
        // still eligible and still failing.
        val catalog = "ret-prefix-prune"
        val f = droppedWithRows(catalog, rows = 20, alsoDropKeeper = true)
        val low = doomedTableId(f.catalogId)
        val high = keeperTableId(f.catalogId)
        // FOUR eligible tables and a page of two, so the page is full,
        // its ceiling is the SECOND id, and the fourth is genuinely
        // beyond the prefix: low < high < beyond < farther.
        val beyond = extraDroppedTable(catalog, f.catalogId, "beyond", 800_000)
        val farther = extraDroppedTable(catalog, f.catalogId, "farther", 700_000)
        assertThat(listOf(low, high, beyond, farther)).isSorted()
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        RetirementGauges.clear()
        try {
            // Two streaks by hand — the arm that earns them is tested
            // elsewhere, and the subject here is the PRUNE: one on the
            // low table (which will be drained out from under the run)
            // and one on the table beyond the page's ceiling.
            RetirementGauges.timedOut(catalog, low)
            RetirementGauges.timedOut(catalog, farther)
            assertThat(streaks(registry, catalog).keys)
                .containsExactlyInAnyOrder(low.toString(), farther.toString())

            // The low table loses its rows to something that is not
            // retirement, so it drops out of the candidate set.
            jdbi.useHandleUnchecked { h ->
                h.createUpdate("DELETE FROM hog_data_file WHERE catalog_id = :c AND table_id = :t")
                    .bind("c", f.catalogId).bind("t", low).execute()
            }

            // The page is [high, beyond] — `low` is no longer eligible —
            // so its ceiling is `beyond` and `farther` is past it.
            val result = service(batch = 100, maxTablesPerRun = 2).runOnce(catalog)
            assertThat(result.tables)
                .describedAs("the run worked the two tables its page did return")
                .isEqualTo(2)
            assertThat(streaks(registry, catalog).keys)
                .describedAs(
                    "%d is below the page's ceiling %d and provably gone, so its series goes; " +
                        "%d is PAST the ceiling, never in scope, and keeps its streak",
                    low,
                    beyond,
                    farther,
                )
                .containsExactly(farther.toString())
        } finally {
            Metrics.clear()
            RetirementGauges.clear()
        }
    }

    @Test
    fun `a tracked table drained out of band above the page ceiling is reconciled away`() {
        // THE THIRD AND LAST SHAPE OF THIS BUG (#282, review pass 2).
        // The ceiling makes a full page safe for the ids BELOW it, but
        // a tracked table ABOVE it is in neither set: the page never
        // reaches it, so it never gets a batch outcome, and the ceiling
        // predicate preserves its streak. Drain it out of band — a
        // manual purge, another instance, an operator's repair — and
        // its 3 alerts forever for work that no longer exists.
        //
        // The page here never reaches either tracked table: three
        // low-id eligible tables against `maxTablesPerRun = 2`, so the
        // ceiling is always the second of them and both tracked ids are
        // past it, run after run.
        //
        // MUTATION: drop `beyondCeilingStillEligible` (pass only the
        // ceiling) and this reds on `drained`, whose series survives
        // its rows. The same case also pins the OTHER direction: a
        // reconciliation that pruned everything past the ceiling would
        // red on `alive`.
        val catalog = "ret-beyond-ceiling"
        val f = droppedWithRows(catalog, rows = 4, alsoDropKeeper = true)
        val third = extraDroppedTable(catalog, f.catalogId, "third", 600_000)
        val drained = extraDroppedTable(catalog, f.catalogId, "drained", 610_000)
        val alive = extraDroppedTable(catalog, f.catalogId, "alive", 620_000)
        assertThat(listOf(doomedTableId(f.catalogId), keeperTableId(f.catalogId), third, drained, alive))
            .describedAs("the two tracked tables must be the HIGHEST ids, past any page of two")
            .isSorted()
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        RetirementGauges.clear()
        try {
            RetirementGauges.timedOut(catalog, drained)
            RetirementGauges.timedOut(catalog, alive)
            assertThat(streaks(registry, catalog).keys)
                .containsExactlyInAnyOrder(drained.toString(), alive.toString())

            // Out of band: nothing in this loop took these rows away.
            jdbi.useHandleUnchecked { h ->
                h.createUpdate("DELETE FROM hog_data_file WHERE catalog_id = :c AND table_id = :t")
                    .bind("c", f.catalogId).bind("t", drained).execute()
            }

            val result = service(batch = 100, maxTablesPerRun = 2).runOnce(catalog)
            assertThat(result.tables)
                .describedAs("the run worked its page of two and never reached either tracked table")
                .isEqualTo(2)
            assertThat(streaks(registry, catalog).keys)
                .describedAs(
                    "%d is past the ceiling AND no longer eligible, so the recheck prunes it; " +
                        "%d is past the ceiling and still eligible, so it keeps its streak",
                    drained,
                    alive,
                )
                .containsExactly(alive.toString())

            // NOT asserted across a second run, deliberately: this run
            // drained the two tables its page DID reach, so the next
            // one's page reaches `alive` and clears its streak through
            // the Retired arm — correct, and a different case.
        } finally {
            Metrics.clear()
            RetirementGauges.clear()
        }
    }

    @Test
    fun `a table whose rows vanish before the first batch drains on the probe and loses its streak`() {
        // THE OTHER COPILOT FINDING. The Drained arm can be a table's
        // FIRST outcome: the probe runs between the candidate read and
        // any committed batch, so rows removed in that window take the
        // table straight to Drained without ever reaching the Retired
        // arm that used to be the only thing clearing a streak. The
        // streak would then survive until some later run's retain
        // happened to prune it — which on a catalog whose pages stay
        // full is the bug above.
        //
        // THE RACE IS STAGED EXACTLY, not approximated. `stampEligible`
        // is the one statement that runs AFTER the candidate read and
        // BEFORE the first batch, so a trigger on its UPDATE fires
        // inside the window the finding is about: the candidate query
        // has already decided the table is eligible (its rows were live
        // then), the retain has already run WITH the table in the set
        // (so it cannot be what clears the streak), and the probe that
        // follows finds nothing live. Deleting the rows before
        // `runOnce` instead would take the table out of the candidate
        // set and test the retain path, which is a different case and
        // already covered.
        //
        // MUTATION: remove `RetirementGauges.recovered` from the Drained
        // arm and this reds — the series outlives the table's rows.
        val catalog = "ret-drain-probe"
        val f = droppedWithRows(catalog, rows = 20)
        val tableId = doomedTableId(f.catalogId)
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        RetirementGauges.clear()
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE FUNCTION drain_on_stamp() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    DELETE FROM hog_data_file
                     WHERE catalog_id = NEW.catalog_id AND table_id = NEW.table_id;
                    RETURN NULL;
                END $$
                """,
            )
            h.execute(
                "CREATE TRIGGER drain_on_stamp AFTER UPDATE OF retirement_eligible_at ON hog_table " +
                    "FOR EACH ROW WHEN (NEW.catalog_id = ${f.catalogId}) " +
                    "EXECUTE FUNCTION drain_on_stamp()",
            )
        }
        try {
            RetirementGauges.timedOut(catalog, tableId)
            assertThat(streaks(registry, catalog)).isEqualTo(mapOf(tableId.toString() to 1.0))

            val result = service(batch = 100).runOnce(catalog)
            assertThat(result.batches)
                .describedAs("nothing was retired: the probe found the table already empty")
                .isZero()
            assertThat(result.timeouts).isZero()
            assertThat(liveFiles(f.catalogId)).isZero()
            assertThat(streaks(registry, catalog))
                .describedAs("a drain is a non-timeout terminal outcome, so the streak ends here")
                .isEmpty()
        } finally {
            jdbi.useHandleUnchecked { h ->
                h.execute("DROP TRIGGER IF EXISTS drain_on_stamp ON hog_table")
                h.execute("DROP FUNCTION IF EXISTS drain_on_stamp()")
            }
            Metrics.clear()
            RetirementGauges.clear()
        }
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
