package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * A catalog drop racing the writers of the same catalog (#412).
 *
 * The property: the two serialize, and the outcome is one of exactly two
 * — the writer loses with a 404, or the drop loses with a typed refusal.
 * Never a 500, never a writer's row erased by a cascade it raced, never a
 * catalog half-deleted (a DELETE is atomic; what this checks is that the
 * PRECONDITIONS and the delete see the same catalog).
 *
 * Interleavings are forced, not hoped for: a SqlLogger hook fires at a
 * named statement inside the first party's transaction — so it holds
 * whatever that transaction holds — starts the second party on another
 * thread (JDBI binds handles to threads), and waits until Postgres
 * reports the second party BLOCKED on a lock before letting the first
 * continue. The `TruncateExpiryRaceIntegrationTest` pattern, plus the
 * wait, which is what makes the interleaving a fact rather than a race.
 *
 * The holder sits IDLE IN TRANSACTION while it waits for the second
 * party to block — that parked transaction is this test's instrument — so
 * the class opts out of PgTestSupport's 2 s idle-in-transaction guard
 * (`holdsTransactions = true`) rather than racing it: with the guard on,
 * a slow machine's [BLOCK_DEADLINE_MS] wait would end in the holder being
 * killed, and the assertion failing for the wrong reason.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogDropConcurrencyIntegrationTest {
    // holdsTransactions: see the class comment — the parked holder IS the
    // instrument, and no production path here holds a transaction idle.
    private val db = PgTestSupport.freshDatabase(holdsTransactions = true)
    private val jdbi: Jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val pool = Executors.newCachedThreadPool()

    @AfterEach
    fun clearHook() {
        jdbi.setSqlLogger(object : SqlLogger {})
    }

    @AfterAll
    fun tearDown() {
        pool.shutdownNow()
        db.close()
    }

    private fun drop(catalog: String) = catalogs.dropCatalog(catalog, "race-test", "drop under contention")

    /**
     * Run [second] on another thread the first time a statement whose SQL
     * contains [marker] is about to execute, and return only once
     * [second] is waiting on a lock whose holder is this backend. The
     * first party then executes its statement and goes on to commit.
     */
    private fun <T> whenStatement(
        marker: String,
        second: () -> T,
    ): AtomicReference<Future<T>> {
        val fired = AtomicBoolean()
        val future = AtomicReference<Future<T>>()
        jdbi.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (!context.rawSql.contains(marker) || !fired.compareAndSet(false, true)) return
                    val holder =
                        context.connection.createStatement().use { st ->
                            st.executeQuery("SELECT pg_backend_pid()").use { rs ->
                                rs.next()
                                rs.getInt(1)
                            }
                        }
                    future.set(pool.submit<T> { second() })
                    awaitBlockedBy(holder)
                }
            },
        )
        return future
    }

    private companion object {
        /**
         * How long the second party gets to reach its lock wait. Generous,
         * because it is a ceiling on a poll that normally returns in
         * milliseconds; the holder parks idle in transaction for up to this
         * long, which is why the class opts out of the idle guard instead of
         * keeping this under it.
         */
        const val BLOCK_DEADLINE_MS = 10_000L
    }

    /** Poll until some backend reports [holder] among the pids blocking it. */
    private fun awaitBlockedBy(holder: Int) {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BLOCK_DEADLINE_MS)
        while (System.nanoTime() < deadline) {
            // A fresh connection, not this thread's: the hook runs on the
            // holder's thread, between its statements.
            val blocked =
                jdbi.open().use { h ->
                    h.createQuery("SELECT count(*) FROM pg_stat_activity WHERE :holder = ANY(pg_blocking_pids(pid))")
                        .bind("holder", holder).mapTo(Long::class.java).one()
                }
            if (blocked > 0) return
            Thread.sleep(5)
        }
        error("the second party never blocked on backend $holder")
    }

    private fun <T> Future<T>.outcome(): Result<T> =
        try {
            Result.success(get(10, TimeUnit.SECONDS))
        } catch (e: ExecutionException) {
            Result.failure(e.cause!!)
        }

    private fun catalogExists(name: String): Boolean =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT count(*) FROM hog_catalog WHERE name = :n").bind("n", name)
                .mapTo(Long::class.java).one() == 1L
        }

    @Test
    fun `a commit queued behind a drop gets a 404, not a 500`() {
        // The drop holds the commit lock (the hook fires on its precondition
        // read); the commit resolved the catalog before queueing, so it
        // wakes up holding the id of a row that is gone. MUTATION: putting
        // CommitTail.read's IllegalStateException back makes this a 500.
        catalogs.createCatalog("race-commit", "s3://bucket/race-commit")
        val commit =
            whenStatement(CatalogRepo.DROP_BLOCKERS_SQL.trim().take(40)) {
                CommitService(jdbi).commit(
                    "race-commit",
                    CommitRequest(
                        appends =
                            listOf(
                                TableAppend(
                                    "ns",
                                    "t",
                                    listOf(FileRegistration("s3://bucket/race-commit/f.parquet", 10, 100)),
                                ),
                            ),
                    ),
                )
            }
        assertThat(drop("race-commit").name).isEqualTo("race-commit")
        val failure = commit.get().outcome().exceptionOrNull()
        assertThat(failure).isInstanceOf(HoglakeException.NotFound::class.java).hasMessageContaining("was dropped")
        assertThat(catalogExists("race-commit")).isFalse()
    }

    @Test
    fun `DDL queued behind a drop gets a 404, not a 500`() {
        // Same race through the DDL tail's first post-lock write.
        // MUTATION: CatalogRepo.allocateSnapshot back to `.one()` makes this
        // an IllegalStateException.
        catalogs.createCatalog("race-ddl", "s3://bucket/race-ddl")
        val create =
            whenStatement(CatalogRepo.DROP_BLOCKERS_SQL.trim().take(40)) {
                catalogs.createNamespace("race-ddl", "late")
            }
        drop("race-ddl")
        assertThat(create.get().outcome().exceptionOrNull())
            .isInstanceOf(HoglakeException.NotFound::class.java).hasMessageContaining("was dropped")
        assertThat(catalogExists("race-ddl")).isFalse()
    }

    @Test
    fun `a drop queued behind DDL sees what the DDL committed and refuses`() {
        // The other order: createNamespace holds the lock (the hook fires on
        // its live-name probe, after the lock); the drop queues, then reads
        // its preconditions AFTER the namespace committed. MUTATION: reading
        // the preconditions before taking the commit lock lets the drop
        // through and cascades the namespace away.
        //
        // The marker relies on createNamespace issuing NO statement containing
        // "FROM hog_namespace" before it takes the commit lock (it reads
        // hog_catalog, locks, then probes the live name). A pre-lock
        // namespace read added there would fire the hook outside the lock,
        // and awaitBlockedBy would then fail with "never blocked".
        catalogs.createCatalog("race-ddl-first", "s3://bucket/race-ddl-first")
        val dropped = whenStatement("FROM hog_namespace") { drop("race-ddl-first") }
        catalogs.createNamespace("race-ddl-first", "won")
        val failure = dropped.get().outcome().exceptionOrNull()
        assertThat(failure).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        assertThat(
            (failure as HoglakeException.CatalogNotEmpty).reason,
        ).isEqualTo(CatalogNotEmptyReason.LIVE_NAMESPACES)
        assertThat(catalogs.getNamespace("race-ddl-first", "won").name).isEqualTo("won")
    }

    @Test
    fun `an upload claim in flight is seen by the drop, not erased by its cascade`() {
        // Upload claims take NO commit lock, so only the catalog row lock
        // (CatalogRepo.lockForDrop) orders them against the drop. The claim
        // is inserted and left uncommitted; the drop must wait for it and
        // then refuse on it. MUTATION: removing lockForDrop's FOR UPDATE lets
        // the precondition read miss the uncommitted claim, and the
        // cascade deletes it after it commits — the drop succeeds and the
        // claim is gone.
        catalogs.createCatalog("race-upload", "s3://bucket/race-upload")
        val catalogId = catalogs.getCatalog("race-upload").catalogId
        val uploadId = UUID.randomUUID()
        jdbi.open().use { claim ->
            claim.begin()
            claim.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind)
                VALUES (:c, :id, :owner, 's3://bucket/race-upload', :path, 'data')
                """,
            ).bind("c", catalogId).bind("id", uploadId).bind("owner", UUID.randomUUID())
                .bind("path", "s3://bucket/race-upload/trino-upload/$uploadId.parquet").execute()
            val holder = claim.createQuery("SELECT pg_backend_pid()").mapTo(Int::class.java).one()
            val dropped = pool.submit<Any> { drop("race-upload") }
            awaitBlockedBy(holder)
            claim.commit()
            val failure = dropped.outcome().exceptionOrNull()
            assertThat(failure).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
            assertThat(
                (failure as HoglakeException.CatalogNotEmpty).reason,
            ).isEqualTo(CatalogNotEmptyReason.ACTIVE_UPLOADS)
        }
        assertThat(catalogExists("race-upload")).isTrue()
        assertThat(
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_upload WHERE upload_id = :id").bind("id", uploadId)
                    .mapTo(Long::class.java).one()
            },
        ).isEqualTo(1)
    }

    @Test
    fun `an upload claim that loses the race is a 404, not a 500`() {
        // Upload claims take no commit lock: the claim resolves the catalog,
        // then its INSERT queues on the drop's row lock and fails its
        // foreign key once the drop commits. MUTATION: removing the
        // Pg.isCatalogForeignKeyViolation mapping in UploadService.inCatalog
        // makes this an UnableToExecuteStatementException (a 500).
        catalogs.createCatalog("race-claim", "s3://bucket/race-claim")
        val claim =
            whenStatement(CatalogRepo.DROP_BLOCKERS_SQL.trim().take(40)) {
                UploadService(
                    jdbi,
                ).claim("race-claim", UUID.randomUUID(), UUID.randomUUID(), "s3://bucket/race-claim", "data")
            }
        drop("race-claim")
        assertThat(claim.get().outcome().exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(catalogExists("race-claim")).isFalse()
    }

    @Test
    fun `an offset commit that loses the race is a 404, not a 500`() {
        // Same race through the other lock-free writer a client calls.
        // MUTATION: removing the mapping in CatalogService.commitOffset.
        catalogs.createCatalog("race-offset", "s3://bucket/race-offset")
        catalogs.createNamespace("race-offset", "ns")
        val table = catalogs.createTable("race-offset", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        catalogs.dropTable("race-offset", "ns", "t", author = "race-test", message = "fixture")
        catalogs.dropNamespace("race-offset", "ns", author = "race-test", message = "fixture")
        val offset =
            whenStatement(CatalogRepo.DROP_BLOCKERS_SQL.trim().take(40)) {
                catalogs.commitOffset("race-offset", "hedgerow-late", table.tableUuid, 1)
            }
        drop("race-offset")
        assertThat(offset.get().outcome().exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(catalogExists("race-offset")).isFalse()
    }

    @Test
    fun `a sampler tick on the catalog being dropped does not deadlock the drop`() {
        // The sampler's order is summary row (FOR UPDATE SKIP LOCKED), then
        // tier INSERTs, whose foreign key takes KEY SHARE on the catalog row.
        // The drop takes the summary row FIRST (CatalogRepo.lockSummaryForDrop),
        // so it waits out the tick. MUTATION: removing that call makes the
        // drop take the catalog row first and wait on the summary row in its
        // cascade, while the tick's tier INSERT waits on the catalog row — a
        // 40P01 after deadlock_timeout, either side the victim.
        catalogs.createCatalog("race-sampler", "s3://bucket/race-sampler")
        val catalogId = catalogs.getCatalog("race-sampler").catalogId
        jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                "INSERT INTO hog_maintenance_summary (catalog_id) VALUES (:c)",
            ).bind("c", catalogId).execute()
        }
        jdbi.open().use { tick ->
            tick.begin()
            tick.createQuery(
                "SELECT catalog_id FROM hog_maintenance_summary WHERE catalog_id = :c FOR UPDATE SKIP LOCKED",
            )
                .bind("c", catalogId).mapTo(Long::class.java).one()
            val holder = tick.createQuery("SELECT pg_backend_pid()").mapTo(Int::class.java).one()
            val dropped = pool.submit<Any> { drop("race-sampler") }
            awaitBlockedBy(holder)
            tick.createUpdate(
                "INSERT INTO hog_maintenance_summary_tier (catalog_id, generation, bucket_key, table_id, " +
                    "quota, remaining, pending) VALUES (:c, 0, 'b', 1, 1, 1, 0)",
            ).bind("c", catalogId).execute()
            tick.commit()
            assertThat(dropped.outcome().exceptionOrNull()).describedAs("the drop after the tick").isNull()
        }
        assertThat(catalogExists("race-sampler")).isFalse()
    }

    @Test
    fun `a table-creation prepare that loses the race is a 404, not a 500`() {
        // Prepare takes no commit lock: it resolves the catalog and the live
        // namespace, and only then INSERTs its receipt. The hook fires on
        // that INSERT and, on another thread, drops the namespace AND the
        // catalog to completion — prepare holds no lock either would wait
        // on — so the INSERT then fails the receipt's foreign key.
        // MUTATION: removing the Pg.isCatalogForeignKeyViolation mapping in
        // TableCreationService.operationTransaction makes this an
        // UnableToExecuteStatementException (a 500).
        catalogs.createCatalog("race-prepare", "s3://bucket/race-prepare")
        catalogs.createNamespace("race-prepare", "ns")
        val fired = AtomicBoolean()
        jdbi.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (!context.rawSql.contains("INSERT INTO hog_table_creation") ||
                        !fired.compareAndSet(false, true)
                    ) {
                        return
                    }
                    pool.submit {
                        catalogs.dropNamespace("race-prepare", "ns", author = "race-test", message = "fixture")
                        drop("race-prepare")
                    }.get(BLOCK_DEADLINE_MS, TimeUnit.MILLISECONDS)
                }
            },
        )
        val creations = TableCreationService(jdbi, catalogs, CommitService(jdbi))
        assertThatThrownBy {
            creations.prepare(
                "race-prepare",
                UUID.randomUUID(),
                TableCreationDefinition("ns", "t", listOf(ColumnDef("id", ColType.LONG))),
            )
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(fired.get()).isTrue()
        assertThat(catalogExists("race-prepare")).isFalse()
    }

    @Test
    fun `of two concurrent drops one drops and the other is a 404`() {
        catalogs.createCatalog("race-twice", "s3://bucket/race-twice")
        val second = whenStatement(CatalogRepo.DROP_BLOCKERS_SQL.trim().take(40)) { drop("race-twice") }
        assertThat(drop("race-twice").name).isEqualTo("race-twice")
        assertThat(second.get().outcome().exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(catalogExists("race-twice")).isFalse()
    }
}
