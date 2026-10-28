package com.posthog.hoglake.commit

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * The adaptive per-catalog commit pipeline ([CommitPipeline]) against
 * Postgres: ordering and density of snapshots under a pipelined burst,
 * the unchanged durability contract (a response implies committed),
 * idempotent replay of an in-flight commit, OCC under pipelining, the
 * delete path's drain, the cross-pod allocator-race retry, and the
 * quiet-catalog identity with the pre-pipeline path.
 *
 * `CommitPipelineTest` pins the admission/adaptation arithmetic; this
 * class pins what the database sees.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitPipelineIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val counter = AtomicInteger(0)

    /** The service under test: pipeline enabled, generous cap. */
    private val commits = CommitService(db.jdbi, commitMaxInFlight = CommitPipeline.MAX_DEPTH)

    /** Every statement the pipelined service sends, raw SQL — for the lock-form assertions. */
    private val issued = CopyOnWriteArrayList<String>()

    private val instrumented =
        Database.jdbi(db.dataSource).also {
            it.setSqlLogger(
                object : SqlLogger {
                    override fun logAfterExecution(context: StatementContext) {
                        issued += context.rawSql
                    }
                },
            )
        }

    /** A second, instrumented pipelined service (shares the pool, sees the same database). */
    private val instrumentedCommits = CommitService(instrumented, commitMaxInFlight = CommitPipeline.MAX_DEPTH)

    /**
     * The slow-flush / forced-COMMIT-failure fixture: a DEFERRED
     * constraint trigger on hog_data_file fires at COMMIT — inside the
     * committing transaction, BEFORE its row locks are released — and
     * sleeps `delay_ms` (the network-flush stand-in) or raises for the
     * one marked path (the stage-B failure stand-in, exactly once per
     * path: the token sequence is non-transactional, so the consumed
     * failure survives the rollback it causes).
     */
    private fun installCommitGate() {
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "CREATE TABLE IF NOT EXISTS pipe_test_control " +
                    "(id integer PRIMARY KEY, delay_ms integer NOT NULL, fail_path text)",
            )
            h.execute("INSERT INTO pipe_test_control VALUES (1, 0, NULL) ON CONFLICT (id) DO NOTHING")
            h.execute("CREATE SEQUENCE IF NOT EXISTS pipe_test_fail_tokens")
            h.execute(
                """
                CREATE OR REPLACE FUNCTION pipe_test_commit_gate() RETURNS trigger AS ${'$'}fn${'$'}
                DECLARE d integer; fp text;
                BEGIN
                  SELECT delay_ms, fail_path INTO d, fp FROM pipe_test_control WHERE id = 1;
                  IF fp IS NOT NULL AND NEW.path = fp THEN
                    IF nextval('pipe_test_fail_tokens') % 2 = 1 THEN
                      RAISE EXCEPTION 'pipe test commit gate: forced COMMIT failure for %', NEW.path;
                    END IF;
                  END IF;
                  IF d > 0 THEN PERFORM pg_sleep(d / 1000.0); END IF;
                  RETURN NULL;
                END
                ${'$'}fn${'$'} LANGUAGE plpgsql
                """.trimIndent(),
            )
            h.execute("DROP TRIGGER IF EXISTS pipe_test_commit_gate ON hog_data_file")
            h.execute(
                "CREATE CONSTRAINT TRIGGER pipe_test_commit_gate AFTER INSERT ON hog_data_file " +
                    "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pipe_test_commit_gate()",
            )
        }
    }

    private fun setGate(
        delayMs: Int,
        failPath: String? = null,
    ) = db.jdbi.useHandleUnchecked { h ->
        h.execute("UPDATE pipe_test_control SET delay_ms = ?, fail_path = ? WHERE id = 1", delayMs, failPath)
    }

    /** Granted advisory locks of the commit-lock class, across every backend. Zero when idle and healthy. */
    private fun advisoryLocksHeld(): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted " +
                    "AND classid = ${com.posthog.hoglake.persistence.Locks.CATALOG_COMMIT_LOCK_CLASS}",
            ).mapTo(Long::class.java).one()
        }

    /**
     * One contended burst with the commit gate delaying COMMITs, leaving
     * the catalog's pipe widened (depth >= 2 survives the halving
     * drain), so the NEXT commit takes the PIPELINED path even alone.
     */
    private fun warmPipe(cat: String) {
        setGate(200)
        try {
            val pool = Executors.newFixedThreadPool(4)
            val tasks = (1..4).map { pool.submit { commits.commit(cat, append(cat)) } }
            pool.shutdown()
            tasks.forEach { it.get(30, TimeUnit.SECONDS) }
        } finally {
            setGate(0)
        }
    }

    @org.junit.jupiter.api.BeforeAll
    fun installGate() = installCommitGate()

    @org.junit.jupiter.api.AfterEach
    fun resetGate() = setGate(0)

    private fun fixture(): String {
        val cat = "pipe-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ingest")
        catalogs.createTable(cat, "ingest", "events", listOf(ColumnDef("id", ColType.LONG)))
        return cat
    }

    private fun head(cat: String): Long = catalogs.getCatalog(cat).headSnapshotId

    /** A one-file blind append to `ingest.events` — the quiet-hour ingest shape. */
    private fun append(
        cat: String,
        key: UUID? = null,
        records: Long = 100,
    ) = CommitRequest(
        idempotencyKey = key,
        appends =
            listOf(
                TableAppend(
                    "ingest",
                    "events",
                    listOf(
                        FileRegistration(
                            path = "s3://bucket/$cat/data/${UUID.randomUUID()}.parquet",
                            recordCount = records,
                            fileSizeBytes = 4_096,
                            footerSize = 512,
                        ),
                    ),
                ),
            ),
    )

    /** (data_file_id, record_count) of every events file, in id order. */
    private fun files(cat: String): List<Pair<Long, Long>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.data_file_id, f.record_count FROM hog_data_file f
                JOIN hog_catalog c USING (catalog_id) WHERE c.name = :cat ORDER BY f.data_file_id
                """,
            ).bind("cat", cat).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.list()
        }

    /** (row_id_start, record_count, begin_snapshot) of every events file, in row-id order. */
    private fun rowIdTiles(cat: String): List<Triple<Long, Long, Long>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.row_id_start, f.record_count, f.begin_snapshot FROM hog_data_file f
                JOIN hog_catalog c USING (catalog_id) WHERE c.name = :cat ORDER BY f.row_id_start
                """,
            ).bind("cat", cat).map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }.list()
        }

    private fun snapshotCount(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM hog_snapshot s JOIN hog_catalog c USING (catalog_id) WHERE c.name = :cat",
            ).bind("cat", cat).mapTo(Long::class.java).one()
        }

    @Test
    fun `a pipelined burst keeps snapshot ids dense and ordered, row ids tiled, and every response durable`() {
        val cat = fixture()
        val base = head(cat)
        val writers = 8
        val perWriter = 10
        val results = ConcurrentLinkedQueue<CommitResult>()
        val pool = Executors.newFixedThreadPool(writers)
        val tasks =
            (1..writers).map {
                pool.submit {
                    repeat(perWriter) {
                        val result = commits.commit(cat, append(cat))
                        // THE DURABILITY CONTRACT: the response implies the
                        // commit's WAL record flushed, so the catalog head is
                        // already at or past the response's snapshot — never
                        // acked early, pipelining or not.
                        val committedHead = head(cat)
                        assertThat(committedHead)
                            .describedAs("head visible when the response for snapshot ${result.snapshotId} went out")
                            .isGreaterThanOrEqualTo(result.snapshotId)
                        results += result
                    }
                }
            }
        pool.shutdown()
        tasks.forEach { it.get(120, TimeUnit.SECONDS) }

        // Dense and ordered: the responses are exactly the base+1..base+N range.
        val ids = results.map { it.snapshotId }.sorted()
        assertThat(ids).hasSize(writers * perWriter)
        assertThat(ids).isEqualTo((base + 1..base + writers * perWriter).toList())

        // Lineage: row-id ranges tile [0, total) with no overlap and no gap,
        // and each file's begin_snapshot is one of the responses'.
        val tiles = rowIdTiles(cat)
        assertThat(tiles).hasSize(writers * perWriter)
        var expected = 0L
        for ((start, count, _) in tiles) {
            assertThat(start).isEqualTo(expected)
            expected += count
        }
        assertThat(tiles.map { it.third }.toSet()).isEqualTo(ids.toSet())
    }

    @Test
    fun `concurrent replays of one in-flight commit publish exactly once`() {
        val cat = fixture()
        val base = head(cat)
        val key = UUID.randomUUID()
        val request = append(cat, key)
        val snapshotsBefore = snapshotCount(cat)
        val pool = Executors.newFixedThreadPool(8)
        val results = ConcurrentLinkedQueue<CommitResult>()
        val tasks =
            (1..8).map {
                pool.submit { results += commits.commit(cat, request) }
            }
        pool.shutdown()
        tasks.forEach { it.get(60, TimeUnit.SECONDS) }

        // Every caller got THE publication's answer; the catalog advanced once.
        assertThat(results.map { it.snapshotId }.distinct()).containsExactly(base + 1)
        assertThat(snapshotCount(cat)).isEqualTo(snapshotsBefore + 1)
        assertThat(files(cat)).hasSize(1)
        // A late replay answers from the durable receipt, identically.
        assertThat(commits.commit(cat, request).snapshotId).isEqualTo(base + 1)
        // A different payload under the same key is still refused.
        assertThatThrownBy { commits.commit(cat, append(cat, key)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("idempotency_key reused with a different request")
    }

    @Test
    fun `a guarded delete racing pipelined appends gets its 409, never a silent write`() {
        val cat = fixture()
        commits.commit(cat, append(cat))
        val target = files(cat).single().first
        val readSnap = head(cat)

        // One change lands AFTER the read snapshot, synchronously: the
        // conflict is owed deterministically.
        commits.commit(cat, append(cat))

        // More appends in flight while the deletes run: the delete path
        // drains them before reading, so every one of these sees the
        // change rows and refuses — none may slip a write past the
        // guard. Submissions interleave so deletes genuinely race the
        // burst.
        val pool = Executors.newFixedThreadPool(24)
        val appendTasks = java.util.Collections.synchronizedList(mutableListOf<java.util.concurrent.Future<*>>())
        val deleteTasks =
            java.util.Collections.synchronizedList(
                mutableListOf<java.util.concurrent.Future<Pair<String, String>?>>(),
            )
        for (i in 1..16) {
            appendTasks += pool.submit { commits.commit(cat, append(cat)) }
            if (i <= 8) {
                deleteTasks +=
                    pool.submit<Pair<String, String>?> {
                        try {
                            commits.commit(
                                cat,
                                CommitRequest(
                                    readSnapshot = readSnap,
                                    requireUnchangedTables = true,
                                    deletes =
                                        listOf(
                                            TableDeletes(
                                                "ingest",
                                                "events",
                                                listOf(
                                                    DeleteFileRegistration(
                                                        target,
                                                        "s3://bucket/$cat/dv/${UUID.randomUUID()}.puffin",
                                                        10,
                                                        64,
                                                    ),
                                                ),
                                            ),
                                        ),
                                ),
                            )
                            null
                        } catch (e: HoglakeException.CommitConflict) {
                            "conflict" to ""
                        }
                    }
            }
        }
        pool.shutdown()
        appendTasks.forEach { it.get(60, TimeUnit.SECONDS) }
        val deleteOutcomes = deleteTasks.map { it.get(60, TimeUnit.SECONDS) }
        assertThat(deleteOutcomes).allMatch { it != null }
        // No DV was ever registered: the target still has no live vector.
        val dvCount =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT count(*) FROM hog_delete_file d JOIN hog_catalog c USING (catalog_id)
                    WHERE c.name = :cat AND d.end_snapshot IS NULL
                    """,
                ).bind("cat", cat).mapTo(Long::class.java).one()
            }
        assertThat(dvCount).isZero()
    }

    @Test
    fun `a delete commit through an enabled pipeline commits and supersedes exactly as before`() {
        val cat = fixture()
        commits.commit(cat, append(cat))
        val target = files(cat).single().first
        val snap = head(cat)
        val result =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = snap,
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ingest",
                                "events",
                                listOf(DeleteFileRegistration(target, "s3://bucket/$cat/dv/one.puffin", 10, 64)),
                            ),
                        ),
                ),
            )
        assertThat(result.snapshotId).isEqualTo(snap + 1)
        // And a second, growing vector supersedes it — the DRAINED path
        // keeps the whole supersession chain intact.
        val second =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = result.snapshotId,
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ingest",
                                "events",
                                listOf(DeleteFileRegistration(target, "s3://bucket/$cat/dv/two.puffin", 20, 64)),
                            ),
                        ),
                ),
            )
        assertThat(second.snapshotId).isEqualTo(snap + 2)
        val live =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT d.path, d.delete_count FROM hog_delete_file d
                    JOIN hog_catalog c USING (catalog_id)
                    WHERE c.name = :cat AND d.end_snapshot IS NULL
                    """,
                ).bind("cat", cat).map { rs, _ -> rs.getString(1) to rs.getLong(2) }.list()
            }
        assertThat(live).containsExactly("s3://bucket/$cat/dv/two.puffin" to 20L)
    }

    @Test
    fun `an allocator moved between the read and the write is retried, not failed`() {
        // CommitTailGuardIntegrationTest's mutation — a writer outside the
        // commit lock, the cross-pod race's stand-in — but with the
        // pipeline enabled the commit RETRIES whole and lands.
        val cat = fixture()
        val fired = AtomicBoolean()
        val rogue =
            Database.jdbi(db.dataSource).also {
                it.setSqlLogger(
                    object : SqlLogger {
                        override fun logAfterExecution(context: StatementContext) {
                            if (context.rawSql == CommitTail.READ_SQL && fired.compareAndSet(false, true)) {
                                Executors.newSingleThreadExecutor().use { executor ->
                                    executor.submit {
                                        db.jdbi.useHandleUnchecked { h ->
                                            h.execute(
                                                "UPDATE hog_catalog SET last_snapshot_id = last_snapshot_id + 1 " +
                                                    "WHERE name = '$cat'",
                                            )
                                        }
                                    }.get(10, TimeUnit.SECONDS)
                                }
                            }
                        }
                    },
                )
            }
        val raced = CommitService(rogue, commitMaxInFlight = CommitPipeline.MAX_DEPTH)
        val before = snapshotCount(cat)
        val result = raced.commit(cat, append(cat))
        // The rogue ate one id; the commit's snapshot is the one after.
        assertThat(snapshotCount(cat)).isEqualTo(before + 1)
        assertThat(files(cat)).hasSize(1)
        assertThat(head(cat)).isEqualTo(result.snapshotId)
    }

    @Test
    fun `a quiet catalog takes the xact-locked path, exactly as without the pipeline`() {
        val cat = fixture()
        issued.clear()
        instrumentedCommits.commit(cat, append(cat))
        instrumentedCommits.commit(cat, append(cat))
        val locks = issued.filter { it.contains("pg_advisory") }
        assertThat(locks).describedAs("lock statements for two quiet commits").allMatch {
            it.contains("pg_advisory_xact_lock")
        }
        assertThat(locks).noneMatch { it.contains("pg_advisory_unlock") }

        // A delete commit adds exactly the drain, still under the xact lock.
        val target = files(cat).first().first
        issued.clear()
        instrumentedCommits.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                requireUnchangedTables = true,
                deletes =
                    listOf(
                        TableDeletes(
                            "ingest",
                            "events",
                            listOf(DeleteFileRegistration(target, "s3://bucket/$cat/dv/q.puffin", 5, 64)),
                        ),
                    ),
            ),
        )
        assertThat(issued).anyMatch { it.contains("FOR NO KEY UPDATE") }
        assertThat(issued.filter { it.contains("pg_advisory") }).allMatch { it.contains("pg_advisory_xact_lock") }
    }

    @Test
    fun `a contended catalog pipelines — session-scoped stage-A locks appear under a burst`() {
        val cat = fixture()
        issued.clear()
        val pool = Executors.newFixedThreadPool(8)
        val tasks =
            (1..40).map {
                pool.submit { instrumentedCommits.commit(cat, append(cat)) }
            }
        pool.shutdown()
        tasks.forEach { it.get(120, TimeUnit.SECONDS) }
        // The first commit of the burst may still take the quiet path;
        // contended ones must pipeline (session lock + unlock riding the write).
        assertThat(issued).anyMatch { it.contains("pg_advisory_lock(") }
        assertThat(issued.filter { it.contains("pg_advisory_lock(") }).allMatch {
            !it.contains("pg_advisory_xact_lock")
        }
        assertThat(issued).anyMatch { it.contains("pg_advisory_unlock") }
        // And every commit landed: no retry or wedged lock anywhere.
        assertThat(files(cat)).hasSize(40)
    }

    @Test
    fun `lock timeouts under contention leak no session advisory locks onto the pool`() {
        // The P0 repro: Postgres can GRANT pg_advisory_lock and then fail
        // the statement (the lock_timeout interrupt landing after the
        // grant); a session lock survives the rollback, and a pooled
        // connection carrying it wedges the catalog. A tiny admission
        // bound under heavy contention produces that race in bulk; the
        // validation-failure mix also sweeps the post-acquire error
        // path. The pin: once idle, pg_locks must hold NO granted
        // commit-class advisory lock, and the catalog must still commit.
        val cat = fixture()
        val squeezed = CommitService(db.jdbi, commitLockTimeoutMs = 3, commitMaxInFlight = CommitPipeline.MAX_DEPTH)
        val unexpected = ConcurrentLinkedQueue<Throwable>()
        val pool = Executors.newFixedThreadPool(24)
        val tasks =
            (1..24).map { w ->
                pool.submit {
                    repeat(40) { i ->
                        try {
                            if (i % 7 == 3) {
                                squeezed.commit(
                                    cat,
                                    CommitRequest(
                                        appends =
                                            listOf(
                                                TableAppend("ingest", "missing_$i", listOf(fileRegistration(cat))),
                                            ),
                                    ),
                                )
                            } else {
                                squeezed.commit(cat, append(cat))
                            }
                        } catch (e: HoglakeException.CommitQueueTimeout) {
                            // the typed 503 the admission contract promises
                        } catch (e: HoglakeException.Validation) {
                            // missing table: the 422 it always was
                        } catch (e: HoglakeException.CommitConflict) {
                            // unreachable here, but typed backpressure all the same
                        } catch (e: Throwable) {
                            unexpected += e
                        }
                    }
                }
            }
        pool.shutdown()
        tasks.forEach { it.get(120, TimeUnit.SECONDS) }
        assertThat(unexpected).isEmpty()
        assertThat(advisoryLocksHeld()).describedAs("granted commit-class advisory locks once idle").isZero()
        // Not wedged: a fresh commit with a sane budget goes through.
        commits.commit(cat, append(cat))
    }

    @Test
    fun `a failure at the successful-acquire boundary leaks no session lock`() {
        // Fault injection on the acquire's return path (SQL logging,
        // metrics, result handling all run after the lock can be held):
        // the SqlLogger throws AFTER the session acquire's statement
        // executed. The driver must still unlock before the connection
        // re-enters the pool.
        val cat = fixture()
        val saboteur =
            Database.jdbi(db.dataSource).also {
                it.setSqlLogger(
                    object : SqlLogger {
                        override fun logAfterExecution(context: StatementContext) {
                            if (context.rawSql.startsWith("SELECT pg_advisory_lock")) {
                                throw RuntimeException("injected post-acquire failure")
                            }
                        }
                    },
                )
            }
        val sabotaged = CommitService(saboteur, commitMaxInFlight = CommitPipeline.MAX_DEPTH)
        // Force the PIPELINED path: one delayed commit holds the permit,
        // the second contends and takes the session lock.
        setGate(400)
        val pool = Executors.newFixedThreadPool(2)
        val leader = pool.submit { sabotaged.commit(cat, append(cat)) }
        Thread.sleep(100) // leader is in stage B; the follower pipelines
        val follower =
            pool.submit<Pair<Long, Throwable?>?> {
                try {
                    sabotaged.commit(cat, append(cat)).snapshotId to null
                } catch (e: Throwable) {
                    null
                }
            }
        pool.shutdown()
        follower.get(30, TimeUnit.SECONDS)
        setGate(0)
        leader.get(30, TimeUnit.SECONDS)
        assertThat(advisoryLocksHeld()).describedAs("granted commit-class advisory locks once idle").isZero()
        // And the catalog still commits (the leader landed; nothing wedges).
        commits.commit(cat, append(cat))
    }

    @Test
    fun `the pipelined write runs with the session lock held — the handoff is after the write`() {
        // A BEFORE UPDATE trigger on hog_catalog that requires THIS
        // backend to hold the catalog's advisory lock at the alloc
        // UPDATE. An unlock CTE was observed (PG 18) to fire BEFORE the
        // write CTEs; the explicit post-write unlock statement this
        // trigger pins cannot drift that way. Legacy xact-locked
        // commits pass the same check (their lock shows in pg_locks
        // during the write).
        val cat = fixture()
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE OR REPLACE FUNCTION pipe_handoff_check() RETURNS trigger AS ${'$'}fn${'$'}
                BEGIN
                  PERFORM 1 FROM pg_locks
                   WHERE locktype = 'advisory' AND granted
                     AND classid = ${com.posthog.hoglake.persistence.Locks.CATALOG_COMMIT_LOCK_CLASS}
                     AND objid = ((OLD.catalog_id & 4294967295)::integer)::oid
                     AND pid = pg_backend_pid();
                  IF NOT FOUND THEN
                    RAISE EXCEPTION 'stage A unlocked before the alloc UPDATE (catalog_id=%)', OLD.catalog_id;
                  END IF;
                  RETURN NEW;
                END
                ${'$'}fn${'$'} LANGUAGE plpgsql
                """.trimIndent(),
            )
            h.execute(
                "CREATE TRIGGER pipe_handoff BEFORE UPDATE ON hog_catalog " +
                    "FOR EACH ROW EXECUTE FUNCTION pipe_handoff_check()",
            )
        }
        try {
            warmPipe(cat) // leaves depth >= 2, so the next commit pipelines
            commits.commit(cat, append(cat))
            // A contended pair, for the session-locked path under
            // pressure as well.
            setGate(300)
            val pool = Executors.newFixedThreadPool(2)
            val tasks = (1..2).map { pool.submit { commits.commit(cat, append(cat)) } }
            pool.shutdown()
            tasks.forEach { it.get(30, TimeUnit.SECONDS) }
        } finally {
            setGate(0)
            db.jdbi.useHandleUnchecked { h ->
                h.execute("DROP TRIGGER IF EXISTS pipe_handoff ON hog_catalog")
                h.execute("DROP FUNCTION IF EXISTS pipe_handoff_check()")
            }
        }
    }

    @Test
    fun `slow flush — the guard waits instead of retrying, and pipelining is not slower than serial`() {
        // The P0-2 pin. With COMMIT artificially slowed (the network
        // flush stand-in), overlapping pipelined commits must WAIT on
        // the predecessor's row lock and proceed — one write statement
        // per commit, zero AllocationMoved retries — and the pipelined
        // burst must not be slower than the pre-pipeline path. (What
        // pipelining overlaps is the read+checks with the predecessor's
        // flush; the flushes themselves stay serial through the row
        // lock — see CommitPipeline's KDoc.)
        val serialCat = fixture()
        val pipeCat = fixture()
        val serial = CommitService(db.jdbi) // pipeline off: the pre-pipeline path
        val writers = 12
        val perWriter = 10
        val n = writers * perWriter

        fun burst(
            service: CommitService,
            catalog: String,
        ): Long {
            val started = System.nanoTime()
            val pool = Executors.newFixedThreadPool(writers)
            val tasks =
                (1..writers).map {
                    pool.submit { repeat(perWriter) { service.commit(catalog, append(catalog)) } }
                }
            pool.shutdown()
            tasks.forEach { it.get(300, TimeUnit.SECONDS) }
            return (System.nanoTime() - started) / 1_000_000
        }

        setGate(50)
        try {
            val serialMs = burst(serial, serialCat)
            issued.clear()
            val pipelinedMs = burst(instrumentedCommits, pipeCat)
            val writes = issued.count { it == CommitTail.WRITE_SQL }
            println("SLOW_FLUSH_BENCH serialMs=$serialMs pipelinedMs=$pipelinedMs commits=$n")
            assertThat(writes)
                .describedAs("one write statement per commit — a retry would re-run it")
                .isEqualTo(n)
            assertThat(pipelinedMs)
                .describedAs("pipelined ($pipelinedMs ms) must not be slower than serial ($serialMs ms)")
                .isLessThanOrEqualTo((serialMs * 1.05).toLong() + 250)
        } finally {
            setGate(0)
        }
        // Both landed dense and complete.
        assertThat(files(serialCat)).hasSize(n)
        assertThat(files(pipeCat)).hasSize(n)
        assertThat(rowIdTiles(pipeCat).map { it.third }.toSet().size).isEqualTo(n)
    }

    @Test
    fun `a guarded delete waits out an in-flight pipelined append and conflicts — the drain is pinned`() {
        // Without drainInFlightCommits this test FAILS: the delete's
        // read would miss the in-flight append, its write would block on
        // the append's row lock, find the allocator moved after the
        // append's commit, and die as a bug-classified 500 — not the
        // 409 the OCC contract owes. With the drain, the delete waits
        // out the append's COMMIT on the row lock BEFORE reading, sees
        // the change, and conflicts.
        val cat = fixture()
        commits.commit(cat, append(cat)) // base file: the delete's target
        val target = files(cat).single().first
        warmPipe(cat) // pipe widened: the next append pipelines
        val readSnap = head(cat)

        setGate(600)
        val pool = Executors.newFixedThreadPool(1)
        val inFlight = pool.submit { commits.commit(cat, append(cat)) }
        pool.shutdown()
        Thread.sleep(150) // the append is in stage B: advisory released, COMMIT delayed
        val started = System.nanoTime()
        try {
            assertThatThrownBy {
                commits.commit(
                    cat,
                    CommitRequest(
                        readSnapshot = readSnap,
                        requireUnchangedTables = true,
                        deletes =
                            listOf(
                                TableDeletes(
                                    "ingest",
                                    "events",
                                    listOf(DeleteFileRegistration(target, "s3://bucket/$cat/dv/race.puffin", 10, 64)),
                                ),
                            ),
                    ),
                )
            }.isInstanceOf(HoglakeException.CommitConflict::class.java)
            val waitedMs = (System.nanoTime() - started) / 1_000_000
            assertThat(waitedMs)
                .describedAs("the delete waited out the in-flight commit's flush before reading")
                .isGreaterThanOrEqualTo(300)
        } finally {
            setGate(0)
            inFlight.get(30, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `an in-flight pipelined commit's replay waits for its durability and answers from it`() {
        // The DEFERRED receipt: the original is past its write (delta
        // registered) but its COMMIT is still in flight when the replay
        // arrives, so the replay waits on the original's result rather
        // than publishing a second commit.
        val cat = fixture()
        warmPipe(cat)
        val before = files(cat).size
        val key = UUID.randomUUID()
        val request = append(cat, key)

        setGate(600)
        val pool = Executors.newFixedThreadPool(1)
        val original = pool.submit<CommitResult> { commits.commit(cat, request) }
        pool.shutdown()
        Thread.sleep(150) // original in stage B with its delta registered
        val started = System.nanoTime()
        try {
            val replayed = commits.commit(cat, request)
            val waitedMs = (System.nanoTime() - started) / 1_000_000
            assertThat(waitedMs)
                .describedAs("the replay waited for the in-flight original's durability")
                .isGreaterThanOrEqualTo(300)
            assertThat(replayed.snapshotId).isEqualTo(original.get(30, TimeUnit.SECONDS).snapshotId)
        } finally {
            setGate(0)
        }
        // Exactly one commit published: one file, one snapshot.
        assertThat(files(cat)).hasSize(before + 1)
        // And a late replay answers the same from the durable receipt.
        assertThat(commits.commit(cat, request).snapshotId).isEqualTo(original.get().snapshotId)
    }

    @Test
    fun `an in-flight commit whose COMMIT fails lets its replay publish anew, exactly once`() {
        // Stage-B failure: the original's COMMIT raises (the gate's
        // one-shot token makes the SAME payload's retry pass), its delta
        // is failed and forgotten, and the waiting replay publishes its
        // payload itself — dense, once, with no phantom snapshot.
        val cat = fixture()
        warmPipe(cat)
        val base = head(cat)
        val before = files(cat).size
        val path = "s3://bucket/$cat/data/${UUID.randomUUID()}.parquet"
        val key = UUID.randomUUID()
        val request =
            CommitRequest(
                idempotencyKey = key,
                appends = listOf(TableAppend("ingest", "events", listOf(fileRegistration(cat, path)))),
            )
        db.jdbi.useHandleUnchecked { h -> h.execute("ALTER SEQUENCE pipe_test_fail_tokens RESTART WITH 1") }
        setGate(300, failPath = path)

        val pool = Executors.newFixedThreadPool(1)
        val original =
            pool.submit<Pair<Long, Throwable?>?> {
                try {
                    commits.commit(cat, request).snapshotId to null
                } catch (e: Throwable) {
                    null
                }
            }
        pool.shutdown()
        Thread.sleep(150) // original in stage B; its COMMIT will raise
        try {
            val published = commits.commit(cat, request)
            // The replay woke from the original's failure and published
            // the payload itself, at the next dense snapshot.
            assertThat(published.snapshotId).isEqualTo(base + 1)
        } finally {
            setGate(0)
            original.get(30, TimeUnit.SECONDS) // failed; nothing to assert about its exception type
        }
        assertThat(files(cat)).hasSize(before + 1)
        assertThat(head(cat)).isEqualTo(base + 1)
        // The durable receipt now answers replays of the published commit.
        assertThat(commits.commit(cat, request).snapshotId).isEqualTo(base + 1)
    }

    @Test
    fun `a drain wait out of admission budget is the typed 503, not a 500`() {
        // The drain's FOR NO KEY UPDATE honors the transaction's
        // lock_timeout; its expiry is the same admission backpressure
        // as the advisory queue's — CommitQueueTimeout, not a raw
        // UnableToExecuteStatementException (500).
        val cat = fixture()
        commits.commit(cat, append(cat))
        val target = files(cat).single().first
        warmPipe(cat)
        val readSnap = head(cat)
        val squeezed = CommitService(db.jdbi, commitLockTimeoutMs = 100, commitMaxInFlight = CommitPipeline.MAX_DEPTH)

        setGate(600)
        val pool = Executors.newFixedThreadPool(1)
        val inFlight = pool.submit { commits.commit(cat, append(cat)) }
        pool.shutdown()
        Thread.sleep(150) // the append is in stage B, holding the row lock
        try {
            assertThatThrownBy {
                squeezed.commit(
                    cat,
                    CommitRequest(
                        readSnapshot = readSnap,
                        requireUnchangedTables = true,
                        deletes =
                            listOf(
                                TableDeletes(
                                    "ingest",
                                    "events",
                                    listOf(DeleteFileRegistration(target, "s3://bucket/$cat/dv/t.puffin", 10, 64)),
                                ),
                            ),
                    ),
                )
            }.isInstanceOf(HoglakeException.CommitQueueTimeout::class.java)
        } finally {
            setGate(0)
            inFlight.get(30, TimeUnit.SECONDS)
        }
        assertThat(advisoryLocksHeld()).isZero()
    }

    private fun fileRegistration(
        cat: String,
        path: String = "s3://bucket/$cat/data/${UUID.randomUUID()}.parquet",
    ) = FileRegistration(path = path, recordCount = 100, fileSizeBytes = 4_096, footerSize = 512)
}
