package com.posthog.hoglake.persistence

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

/**
 * The commit lock's in-process series (#439) against REAL callers and
 * real JDBI transactions: each kind of work records its hold under its
 * own `path`, the hold closes on commit AND on rollback, and a real
 * `lock_timeout` leaves nobody counted as waiting.
 *
 * The unit contract (stubbed acquire, stubbed end) is
 * `CommitLockObserverTest`; which path every call site passes is pinned
 * by `CommitLockCallSitesTest`. This class is what proves the JDBI hooks
 * actually fire on the transactions those callers open.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitLockHoldIntegrationTest {
    // The holders sit idle in transaction on purpose; the suite's 2 s
    // idle-in-transaction guard would kill them mid-convoy.
    private val db = PgTestSupport.freshDatabase(holdsTransactions = true)
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val counter = AtomicInteger()
    private lateinit var registry: SimpleMeterRegistry

    private val minio: MinIOContainer by lazy { TestImages.minio().also { it.start() } }
    private val store: ObjectStore by lazy {
        ObjectStore(
            endpoint = minio.s3URL,
            region = "us-east-1",
            accessKey = minio.userName,
            secretKey = minio.password,
            pathStyle = true,
        ).also { it.createBucket(BUCKET) }
    }

    private companion object {
        const val BUCKET = "hoglake-lock-hold-test"
    }

    @BeforeEach
    fun bind() {
        registry = SimpleMeterRegistry()
        Metrics.bind(registry)
    }

    @AfterEach
    fun unbind() = Metrics.clear()

    @AfterAll
    fun tearDown() {
        db.close()
        if (minio.isRunning) minio.stop()
    }

    private fun holds(
        catalog: String,
        path: String,
    ): Long = registry.find("hoglake_commit_lock_hold").tags("catalog", catalog, "path", path).timer()?.count() ?: 0

    private fun holdPaths(catalog: String): Map<String, Long> =
        registry.find("hoglake_commit_lock_hold").tag("catalog", catalog).timers()
            .associate { it.id.getTag("path")!! to it.count() }

    private fun gauge(
        name: String,
        catalog: String,
    ): Double? = registry.find(name).tag("catalog", catalog).gauge()?.value()

    private fun table(): String {
        val cat = "hold-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        return cat
    }

    private fun commitOne(
        cat: String,
        path: String,
        rows: Long = 10,
        bytes: Long = 1000,
    ) = commits.commit(
        cat,
        CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, rows, bytes))))),
    )

    @Test
    fun `a data commit records its hold under commit and releases the held gauge`() {
        val cat = table()
        val ddl = holdPaths(cat)
        // The setup's own DDL is labelled ddl, never commit.
        assertThat(ddl).containsOnlyKeys("ddl")
        val openBefore = CommitLockObserver.openHolds

        commitOne(cat, "s3://$BUCKET/$cat/f1.parquet")

        assertThat(holds(cat, "commit")).isEqualTo(1)
        assertThat(gauge("hoglake_commit_lock_held", cat)).isEqualTo(0.0)
        assertThat(gauge("hoglake_commit_lock_waiting", cat)).isEqualTo(0.0)
        assertThat(CommitLockObserver.openHolds).isEqualTo(openBefore)
    }

    @Test
    fun `a commit refused under the lock rolls back and still closes its hold`() {
        val cat = table()
        val openBefore = CommitLockObserver.openHolds
        // The table is resolved under the lock, so this is refused there
        // and the transaction rolls back: the afterRollback route.
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "nope", listOf(FileRegistration("s3://$BUCKET/$cat/a.parquet", 1, 1))),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException::class.java)
        assertThat(holds(cat, "commit")).isEqualTo(1)
        assertThat(gauge("hoglake_commit_lock_held", cat)).isEqualTo(0.0)
        assertThat(CommitLockObserver.openHolds).isEqualTo(openBefore)
    }

    @Test
    fun `a compaction group commit records its hold under compaction`() {
        val cat = table()
        val schema: MessageType =
            Types.buildMessage().addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id")).named("t")
        val regs =
            (0 until 2).map { i ->
                val bytes = parquet(schema, listOf(i * 10L, i * 10L + 1))
                val path = "s3://$BUCKET/$cat/data/ns/t/in$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, 2, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val commitsBefore = holds(cat, "commit")
        val cfg = CompactionConfig(targetBytes = 8192, minInputFiles = 2, maxGroupsPerRun = 10)

        assertThat(CompactionService(db.jdbi, store, cfg).runOnce(cat, cfg).groupsCompacted).isEqualTo(1)

        // MUTATION: label CompactionService.commitGroup's acquire COMMIT
        // and the compaction series is absent while commit grows by one.
        assertThat(holds(cat, "compaction")).isEqualTo(1)
        assertThat(holds(cat, "commit")).isEqualTo(commitsBefore)
        assertThat(gauge("hoglake_commit_lock_held", cat)).isEqualTo(0.0)
    }

    @Test
    fun `an expiry floor advance records its hold under expiry`() {
        val cat = table()
        commitOne(cat, "s3://$BUCKET/$cat/f1.parquet")
        val before = holdPaths(cat)

        ExpiryService(db.jdbi).runOnce(cat, 100)

        assertThat(holds(cat, "expiry")).isEqualTo(1)
        // Nothing else took the lock on this catalog for the sweep.
        assertThat(holdPaths(cat).filterKeys { it != "expiry" }).isEqualTo(before)
        assertThat(gauge("hoglake_commit_lock_held", cat)).isEqualTo(0.0)
    }

    @Test
    fun `a drop records its hold under drop`() {
        val cat = table()
        catalogs.dropTable(cat, "ns", "t", author = "test", message = "drop")
        assertThat(holds(cat, "drop")).isEqualTo(1)
    }

    @Test
    fun `a waiter is counted while it queues and uncounted when its lock timeout fires`() {
        val cat = table()
        val catalogId = catalogs.getCatalog(cat).catalogId
        val held = CountDownLatch(1)
        val release = CountDownLatch(1)
        val holder =
            thread(name = "hold-holder") {
                db.jdbi.inTransactionUnchecked { h ->
                    Locks.acquireCatalogCommitLock(h, catalogId, cat, CommitLockPath.COMMIT)
                    held.countDown()
                    release.await(30, TimeUnit.SECONDS)
                }
            }
        try {
            assertThat(held.await(30, TimeUnit.SECONDS)).isTrue()
            assertThat(gauge("hoglake_commit_lock_held", cat)).isEqualTo(1.0)
            val outcome = AtomicReference<Result<Unit>>()
            val waiter =
                thread(name = "hold-waiter") {
                    outcome.set(
                        runCatching {
                            db.jdbi.inTransactionUnchecked { h ->
                                Locks.acquireCatalogCommitLock(h, catalogId, cat, CommitLockPath.COMMIT, 2_000)
                            }
                        },
                    )
                }
            await().atMost(Duration.ofSeconds(10)).untilAsserted {
                assertThat(gauge("hoglake_commit_lock_waiting", cat)).isEqualTo(1.0)
            }
            waiter.join(30_000)
            assertThat(outcome.get().exceptionOrNull()).isInstanceOf(HoglakeException.CommitQueueTimeout::class.java)
            // MUTATION: decrement only on a grant and this stays at 1.
            assertThat(gauge("hoglake_commit_lock_waiting", cat)).isEqualTo(0.0)
            // The holder is untouched by the waiter giving up.
            assertThat(gauge("hoglake_commit_lock_held", cat)).isEqualTo(1.0)
        } finally {
            release.countDown()
            holder.join(30_000)
        }
        assertThat(gauge("hoglake_commit_lock_held", cat)).isEqualTo(0.0)
    }

    private fun parquet(
        schema: MessageType,
        ids: List<Long>,
    ): ByteArray {
        val tmp = Files.createTempFile("lock-hold", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp)).withType(schema).build().use { w ->
                ids.forEach { w.write(factory.newGroup().append("id", it)) }
            }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }
}
