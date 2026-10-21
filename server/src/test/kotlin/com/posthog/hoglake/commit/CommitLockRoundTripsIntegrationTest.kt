package com.posthog.hoglake.commit

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.Measuring
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * How many statements a commit sends while it holds the per-catalog
 * commit lock (#393), and how long it holds it.
 *
 * WHY THE COUNT IS THE ASSERTION. A commit's hold is a FIXED cost per
 * commit — one network round trip per statement, each paid while every
 * other writer of the catalog queues — plus a per-file cost. The
 * 2026-10-14 prod-us collapse (~67 commits/s of small commits, ~43
 * files/s in all, commit p50 ~30 s) was the fixed part: each of those
 * commits sent ~15 statements under the lock, and the lock's per-file
 * duty was ~16 ms/s. Round-trip latency belongs to the network, so no
 * container measures it faithfully; the NUMBER of round trips is a
 * property of the code, and it is what this test pins.
 *
 * Counted on an instrumented Jdbi over the same pool (the
 * `FileStatsApiTest.scanStatements` pattern): every statement after
 * `pg_advisory_xact_lock` returns, to the end of the commit. The lock
 * statement itself and the COMMIT are not counted — the first is where
 * the hold begins, and JDBI does not issue the second as a statement. A
 * JDBC batch counts once; pgjdbc splits a long one into several round
 * trips on its own (a Sync every ~256 queued executions), which is not
 * visible here and is NOT counted, so before #393 the 270-file shape
 * paid more round trips than its count says.
 *
 * Measured on this fixture (PG18 container, local socket), statements
 * under the lock per shape, before -> after #393: see the KDoc of
 * [EXPECTED_UNDER_LOCK] and the printed table.
 *
 * The hold time is MEASURED and PRINTED everywhere and asserted only
 * under `HOGLAKE_MEASURE=1` ([Measuring]), which is also the only place
 * the one-catalog throughput figure runs: both are the machine's
 * numbers, and a local container's round trip is a fraction of RDS's,
 * so neither transfers to production as measured here.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitLockRoundTripsIntegrationTest {
    private companion object {
        /**
         * Statements a SUCCESSFUL commit sends under the lock, whatever
         * its shape: one statement that reads everything the checks need
         * and one that writes everything. Before #393, on these shapes:
         * 19 for a one-file prepared append (and for 270 files, batches
         * counting once), 14 for a blind unpartitioned append, 24 for a
         * two-table append (five more per appended table), 20 for three DV
         * registrations one of which supersedes, 23 for a transaction
         * appending and deleting from its own staged file. A replay was 1
         * and is 1.
         */
        const val EXPECTED_UNDER_LOCK = 2

        /** Files per commit in the large shape: gigahog-prod-us's measured average (#240). */
        const val FILES = 270

        /** Column-stats rows per file: the events table's width. */
        const val COLUMNS = 25

        /** Commits per timed shape; the MEDIAN hold is reported. */
        const val TIMED = 31

        /** Concurrent writers, and commits each, for the throughput figure (`HOGLAKE_MEASURE=1` only). */
        const val WRITERS = 8
        const val COMMITS_PER_WRITER = 60

        /**
         * The 1-file hold bound under HOGLAKE_MEASURE: two statements and a
         * COMMIT on a local socket cannot take 5 ms on a quiet host, and
         * fifteen of them could.
         */
        const val SMALL_HOLD_BUDGET_MS = 5.0
    }

    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    /** Every statement the instrumented Jdbi sends, in order, raw SQL. */
    private val issued = CopyOnWriteArrayList<Pair<Long, String>>()

    private val instrumented =
        Database.jdbi(db.dataSource).also {
            it.setSqlLogger(
                object : SqlLogger {
                    override fun logAfterExecution(context: StatementContext) {
                        issued += System.nanoTime() to context.rawSql
                    }
                },
            )
        }

    /** Same service, instrumented, so the statements it sends are the ones counted. */
    private val commits = CommitService(instrumented)

    @AfterAll
    fun tearDown() = db.close()

    /** A catalog with `ingest.events_raw` (25 longs, identity-partitioned on field 1) and `ingest.sessions`. */
    private fun fixture(): String {
        val cat = "rt-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ingest")
        catalogs.createTable(cat, "ingest", "events_raw", (1..COLUMNS).map { ColumnDef("c$it", ColType.LONG) })
        alter.alterTable(
            cat,
            "ingest",
            "events_raw",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        catalogs.createTable(cat, "ingest", "sessions", listOf(ColumnDef("id", ColType.LONG)))
        return cat
    }

    private fun head(cat: String): Long = catalogs.getCatalog(cat).headSnapshotId

    private fun uuid(
        cat: String,
        table: String,
    ): UUID = catalogs.getTable(cat, "ingest", table, totals = false).tableUuid

    private fun le(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    /** One events_raw file with a stats row per column and an identity partition value. */
    private fun eventsFile(
        cat: String,
        n: Int,
    ): FileRegistration =
        FileRegistration(
            path = "s3://bucket/$cat/data/events_raw/team=${n % 7}/${UUID.randomUUID()}.parquet",
            recordCount = 12_000L + n,
            fileSizeBytes = 48_000L + n * 13,
            footerSize = 4_096,
            columnStats =
                (1..COLUMNS).map { c ->
                    ColumnStats(
                        fieldId = c.toLong(),
                        valueCount = 12_000L + n,
                        nullCount = (n * c % 31).toLong(),
                        nanCount = null,
                        sizeBytes = 900L + c,
                        lowerBound = le(c.toLong()),
                        upperBound = le(c + 1_000L + n),
                    )
                },
            partitionValues = listOf((n % 7).toString()),
            splitOffsets = listOf(4L),
        )

    private fun sessionsFile(cat: String): FileRegistration =
        FileRegistration("s3://bucket/$cat/data/sessions/${UUID.randomUUID()}.parquet", 10, 1_000, 20)

    /** The prepared append every millrace / pyhoglake flush sends: key, read_snapshot, uuid guard. */
    private fun preparedAppend(
        cat: String,
        files: Int,
    ) = CommitRequest(
        readSnapshot = head(cat),
        idempotencyKey = UUID.randomUUID(),
        appends =
            listOf(
                TableAppend("ingest", "events_raw", (1..files).map { eventsFile(cat, it) }, uuid(cat, "events_raw")),
            ),
    )

    /**
     * The statements [action] sent after the commit lock was granted, and
     * the nanoTime the lock statement returned at.
     */
    private fun underLock(action: () -> Unit): Pair<List<String>, Long> {
        issued.clear()
        action()
        val all = issued.toList()
        val lock = all.indexOfFirst { it.second.contains("pg_advisory_xact_lock") }
        check(lock >= 0) { "no commit lock statement was issued: ${all.map { it.second }}" }
        return all.drop(lock + 1).map { it.second } to all[lock].first
    }

    private fun count(action: () -> Unit): Int = underLock(action).first.size

    @Test
    fun `every successful commit shape sends the same two statements under the lock`() {
        val cat = fixture()
        // Seed files to delete from, and a live DV on one of them to supersede.
        commits.commit(cat, preparedAppend(cat, 3))
        val targets =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f JOIN hog_catalog c USING (catalog_id)
                    WHERE c.name = :cat ORDER BY f.data_file_id
                    """,
                ).bind("cat", cat).mapTo(Long::class.java).list()
            }

        fun dv(
            target: Long,
            count: Long,
        ) = DeleteFileRegistration(target, "s3://bucket/$cat/dv/${UUID.randomUUID()}.puffin", count, 64)
        val firstDv =
            CommitRequest(
                readSnapshot = head(cat),
                deletes = listOf(TableDeletes("ingest", "events_raw", listOf(dv(targets[0], 1)))),
            )
        commits.commit(cat, firstDv)

        val oneFile = preparedAppend(cat, 1)
        val shapes =
            linkedMapOf<String, () -> Unit>(
                "prepared append, 1 file x $COLUMNS columns" to { commits.commit(cat, oneFile) },
                "prepared append, $FILES files x $COLUMNS columns" to {
                    commits.commit(cat, preparedAppend(cat, FILES))
                },
                "blind append, 1 unpartitioned file" to {
                    commits.commit(
                        cat,
                        CommitRequest(appends = listOf(TableAppend("ingest", "sessions", listOf(sessionsFile(cat))))),
                    )
                },
                "two-table append" to {
                    commits.commit(
                        cat,
                        CommitRequest(
                            readSnapshot = head(cat),
                            idempotencyKey = UUID.randomUUID(),
                            appends =
                                listOf(
                                    TableAppend("ingest", "events_raw", listOf(eventsFile(cat, 1))),
                                    TableAppend("ingest", "sessions", listOf(sessionsFile(cat))),
                                ),
                        ),
                    )
                },
                "DVs: one superseding a live DV, two fresh" to {
                    commits.commit(
                        cat,
                        CommitRequest(
                            readSnapshot = head(cat),
                            idempotencyKey = UUID.randomUUID(),
                            deletes =
                                listOf(
                                    TableDeletes(
                                        "ingest",
                                        "events_raw",
                                        listOf(dv(targets[0], 2), dv(targets[1], 1), dv(targets[2], 1)),
                                        uuid(cat, "events_raw"),
                                    ),
                                ),
                        ),
                    )
                },
                "transaction: append + a DV on the same commit's file" to {
                    val staged = eventsFile(cat, 2)
                    commits.commit(
                        cat,
                        CommitRequest(
                            readSnapshot = head(cat),
                            idempotencyKey = UUID.randomUUID(),
                            requireUnchangedTables = true,
                            allowPendingDeletes = true,
                            appends =
                                listOf(TableAppend("ingest", "events_raw", listOf(staged), uuid(cat, "events_raw"))),
                            deletes =
                                listOf(
                                    TableDeletes(
                                        "ingest",
                                        "events_raw",
                                        listOf(
                                            DeleteFileRegistration(
                                                0,
                                                "s3://bucket/$cat/dv/${UUID.randomUUID()}.puffin",
                                                1,
                                                64,
                                                dataFilePath = staged.path,
                                            ),
                                        ),
                                        uuid(cat, "events_raw"),
                                    ),
                                ),
                        ),
                    )
                },
            )
        val counts = shapes.mapValues { (_, action) -> count(action) }
        val replay = count { commits.commit(cat, oneFile) }
        println("statements under the commit lock (lock statement and COMMIT excluded):")
        counts.forEach { (shape, n) -> println("  %-55s %d".format(shape, n)) }
        println("  %-55s %d".format("replay of the 1-file append (receipt hit)", replay))

        assertThat(counts)
            .describedAs("statements under the lock, per shape")
            .allSatisfy { _, n -> assertThat(n).isEqualTo(EXPECTED_UNDER_LOCK) }
        // A replay answers from the read statement and writes nothing.
        assertThat(replay).describedAs("a receipt hit sends only the read").isEqualTo(1)
    }

    @Test
    fun `a refused commit sends only the read and writes nothing`() {
        // ddl_since_read_snapshot: before #393, five statements (receipt,
        // head, resolution, spec, conflict scan) before the refusal.
        val cat = fixture()
        val stale = preparedAppend(cat, 1)
        alter.alterTable(cat, "ingest", "events_raw", listOf(AlterOp.SetTableComment("moved")))
        val (statements, _) =
            underLock {
                assertThatThrownBy { commits.commit(cat, stale) }
                    .isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
            }
        assertThat(statements).describedAs("statements under the lock for a refused commit").hasSize(1)
    }

    @Test
    fun `the hold is measured for a small and a production-sized commit`() {
        val cat = fixture()
        // Warm: pgjdbc prepares server-side after five executions, and
        // the first commits of a fresh database pay catalog cache misses.
        repeat(10) { commits.commit(cat, preparedAppend(cat, 1)) }

        fun medianHoldMs(files: Int): Double {
            val holds =
                (1..TIMED).map {
                    val request = preparedAppend(cat, files)
                    val (_, lockedAt) = underLock { commits.commit(cat, request) }
                    (System.nanoTime() - lockedAt) / 1_000_000.0
                }
            return holds.sorted()[TIMED / 2]
        }
        val small = medianHoldMs(1)
        val large = medianHoldMs(FILES)
        println(
            "commit lock hold (lock granted -> commit returned), median of $TIMED: " +
                "1 file x $COLUMNS columns %.2f ms; $FILES files x $COLUMNS columns %.2f ms".format(small, large),
        )
        if (!Measuring.enabled) return
        assertThat(small).describedAs("1-file commit hold, ms").isLessThan(SMALL_HOLD_BUDGET_MS)

        // Throughput of one catalog under contention: the lock serializes
        // every writer, so commits/s is one over the hold plus the hand-off.
        // Measuring-only: it is a printed figure, and WRITERS x
        // COMMITS_PER_WRITER commits is suite time spent on it.
        val plain = CommitService(db.jdbi)
        val requests = (1..WRITERS).map { (1..COMMITS_PER_WRITER).map { preparedAppend(cat, 1) } }
        val pool = Executors.newFixedThreadPool(WRITERS)
        val start = System.nanoTime()
        try {
            requests.map { batch -> pool.submit { batch.forEach { plain.commit(cat, it) } } }
                .forEach { it.get(5, TimeUnit.MINUTES) }
        } finally {
            pool.shutdownNow()
        }
        val seconds = (System.nanoTime() - start) / 1e9
        println(
            "$WRITERS writers x $COMMITS_PER_WRITER one-file commits on one catalog: " +
                "%.0f commits/s".format(WRITERS * COMMITS_PER_WRITER / seconds),
        )
    }
}
