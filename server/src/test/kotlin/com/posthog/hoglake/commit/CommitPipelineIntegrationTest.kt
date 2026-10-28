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
import org.junit.jupiter.api.AfterAll
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

    @AfterAll
    fun tearDown() = db.close()

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
}
