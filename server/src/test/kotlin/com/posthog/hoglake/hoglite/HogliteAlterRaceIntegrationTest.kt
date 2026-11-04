package com.posthog.hoglake.hoglite

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.postgresql.PGConnection
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Same requests and result oracle; only synchronization differs between engines. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteAlterRaceIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private var pg: PgTestSupport.TestDb? = null
    private var engine: HogliteConformanceEngine? = null
    private val mapper = wireObjectMapper()

    @AfterAll
    fun close() {
        pg?.close()
        engine?.close()
    }

    private class Fixture(val store: CatalogStore, val alter: AlterBackend)

    private fun fixture(backend: Backend): Fixture =
        when (backend) {
            Backend.POSTGRES -> {
                // Holding the real production lock is the test's instrument.
                val db = pg ?: PgTestSupport.freshDatabase(holdsTransactions = true).also { pg = it }
                Fixture(
                    PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi)),
                    AlterService(db.jdbi),
                )
            }
            Backend.HOGLITE -> {
                val live = engine ?: HogliteConformanceEngine().also { engine = it }
                Fixture(live.store, HogliteAlters(live.store))
            }
        }

    private data class Outcome(val a: TableInfo, val b: TableInfo?, val failure: Throwable?, val bodies: List<String>)

    private fun race(
        backend: Backend,
        f: Fixture,
        catalog: String,
        opsA: List<AlterOp>,
        opsB: List<AlterOp>,
        readSnapshot: Long? = null,
        uuid: UUID? = null,
        expectedPublications: Int = 2,
    ): Outcome {
        val pool = Executors.newFixedThreadPool(2)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val bodies = CopyOnWriteArrayList<String>()
        val statuses = CopyOnWriteArrayList<Int>()
        val responses = CopyOnWriteArrayList<String>()
        var proxy: HttpServer? = null
        var proxyClient: HogliteClient? = null
        val aPid = AtomicInteger()

        fun publish(
            alter: AlterBackend,
            ops: List<AlterOp>,
            who: String,
        ): TableInfo = alter.alterTable(catalog, "ns", "t", ops, uuid, readSnapshot, who, "message-$who")
        try {
            val a: java.util.concurrent.Future<TableInfo>
            val b: java.util.concurrent.Future<TableInfo>
            if (backend == Backend.POSTGRES) {
                val db = pg!!
                db.jdbi.setSqlLogger(
                    object : SqlLogger {
                        override fun logAfterExecution(context: StatementContext) {
                            if (Thread.currentThread().name == "alter-A" &&
                                "SELECT pg_advisory_xact_lock" in context.rawSql
                            ) {
                                aPid.set(context.connection.unwrap(PGConnection::class.java).backendPID)
                                entered.countDown()
                                check(release.await(20, TimeUnit.SECONDS)) { "release PG ALTER A" }
                            }
                        }
                    },
                )
                a =
                    pool.submit<TableInfo> {
                        Thread.currentThread().name = "alter-A"
                        publish(f.alter, opsA, "A")
                    }
                assertThat(entered.await(20, TimeUnit.SECONDS)).describedAs("A owns production commit lock").isTrue()
                b = pool.submit<TableInfo> { publish(f.alter, opsB, "B") }
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
                var waiting = false
                while (!waiting && System.nanoTime() < deadline) {
                    waiting =
                        db.jdbi.withHandle<Boolean, Exception> { h ->
                            h.createQuery(
                                "SELECT EXISTS (SELECT 1 FROM pg_locks l WHERE l.locktype = 'advisory' " +
                                    "AND NOT l.granted AND :pid = ANY(pg_blocking_pids(l.pid)))",
                            ).bind("pid", aPid.get()).mapTo(Boolean::class.java).one()
                        }
                    if (!waiting) Thread.sleep(10)
                }
                assertThat(waiting).describedAs("B actually waits on A's real advisory lock").isTrue()
            } else {
                val live = engine!!
                val http = HttpClient.newHttpClient()
                proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
                proxy.executor = Executors.newCachedThreadPool()
                proxy.createContext("/") { ex ->
                    try {
                        val body = ex.requestBody.readBytes()
                        val text = body.decodeToString()
                        val alterTx = ex.requestURI.path.endsWith("/tx") && "table_altered" in text
                        if (alterTx) {
                            bodies += text
                            if (bodies.size == 1) {
                                entered.countDown()
                                check(release.await(20, TimeUnit.SECONDS)) { "release fully materialized B" }
                            }
                        }
                        val answer =
                            http.send(
                                HttpRequest.newBuilder(URI.create(live.url() + ex.requestURI))
                                    .header("Content-Type", "application/json")
                                    .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                                HttpResponse.BodyHandlers.ofByteArray(),
                            )
                        if (alterTx) {
                            statuses += answer.statusCode()
                            responses += answer.body().decodeToString()
                        }
                        ex.responseHeaders.add("Content-Type", "application/json")
                        ex.sendResponseHeaders(answer.statusCode(), answer.body().size.toLong())
                        ex.responseBody.use { it.write(answer.body()) }
                    } catch (_: Throwable) {
                        ex.close()
                    }
                }
                proxy.start()
                proxyClient = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}", sleeper = {})
                val store = HogliteCatalogStore(proxyClient).apply { requireOperationBindingForTests = true }
                b = pool.submit<TableInfo> { publish(HogliteAlters(store), opsB, "B") }
                assertThat(entered.await(20, TimeUnit.SECONDS)).describedAs("B fully materialized before A").isTrue()
                a = pool.submit<TableInfo> { publish(f.alter, opsA, "A") }
                a.get(20, TimeUnit.SECONDS)
            }
            release.countDown()
            val resultA = a.get(20, TimeUnit.SECONDS)
            var failure: Throwable? = null
            val resultB =
                try {
                    b.get(20, TimeUnit.SECONDS)
                } catch (e: ExecutionException) {
                    failure = e.cause
                    null
                }
            if (backend == Backend.HOGLITE) {
                assertThat(bodies).describedAs("bounded B materializations").hasSize(expectedPublications)
                assertThat(statuses.first()).describedAs("real engine rolled back stale B").isEqualTo(409)
                // runTx prepends the catalog fence. Pin the REAL failing index,
                // including the extra read_snapshot guard on guarded requests.
                val statements = mapper.readTree(bodies.first())["statements"]
                val field = if (readSnapshot == null) "schema_version" else "snapshot_id >"
                val index = statements.indexOfFirst { field in it["sql"].asText() }
                assertThat(index).isGreaterThan(0)
                assertThat(mapper.readTree(responses.first())["guard_index"].asInt()).isEqualTo(index + 1)
                if (expectedPublications == 2) {
                    assertThat(statuses).containsExactly(409, 200)
                    assertThat(mapper.readTree(bodies[0])["txid"].asText())
                        .isNotEqualTo(mapper.readTree(bodies[1])["txid"].asText())
                }
            }
            return Outcome(resultA, resultB, failure, bodies)
        } finally {
            release.countDown()
            pg?.jdbi?.setSqlLogger(SqlLogger.NOP_SQL_LOGGER)
            proxy?.stop(0)
            (proxy?.executor as? java.util.concurrent.ExecutorService)?.shutdownNow()
            proxyClient?.close()
            pool.shutdownNow()
            assertThat(pool.awaitTermination(20, TimeUnit.SECONDS)).isTrue()
        }
    }

    companion object {
        @JvmStatic
        fun unguardedTargets() =
            Backend.entries.flatMap { backend ->
                listOf(Arguments.of(backend, false), Arguments.of(backend, true))
            }
    }

    @ParameterizedTest(name = "{displayName} / {0} UUID={1}")
    @MethodSource("unguardedTargets")
    fun `independent adds serialize with fresh field ids and own receipts`(
        backend: Backend,
        suppliedUuid: Boolean,
    ) {
        val f = fixture(backend)
        val c = "ar" + UUID.randomUUID().toString().replace("-", "")
        f.store.createCatalog(c, "s3://hoglake-it/$c")
        f.store.createNamespace(c, "ns")
        val original = f.store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val out =
            race(
                backend,
                f,
                c,
                listOf(
                    AlterOp.AddColumn(ColumnDef("a", ColType.LONG)),
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY))),
                ),
                listOf(
                    AlterOp.AddColumn(ColumnDef("b", ColType.LONG)),
                    AlterOp.RenameColumn("b", "c"),
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY))),
                ),
                uuid = if (suppliedUuid) original.tableUuid else null,
            )
        assertThat(out.failure).isNull()
        val b = out.b!!
        assertThat(b.columns.map { it.def.name }).containsExactly("id", "a", "c")
        assertThat(b.columns.map { it.fieldId }).containsExactly(1, 2, 3)
        assertThat(b.tableUuid).isEqualTo(original.tableUuid)
        assertThat(b.readSnapshotId).isEqualTo(out.a.readSnapshotId + 1)
        assertThat(b.snapshotId).isEqualTo(b.readSnapshotId)
        assertThat(b.fileCount).isZero()
        assertThat(out.a.partitionSpec!!.specId).isEqualTo(1)
        assertThat(b.partitionSpec!!.specId).isEqualTo(2)
        assertThat(b.partitionSpec!!.fields.single().sourceFieldId).isEqualTo(1)
        assertThat(f.store.getTable(c, "ns", "t", snapshot = out.a.readSnapshotId).columns.map { it.def.name })
            .containsExactly("id", "a")
        val snapshots = f.store.listSnapshots(c, original.readSnapshotId, 100).first
        assertThat(snapshots).hasSize(2)
        assertThat(snapshots.map { it.author }).containsExactly("A", "B")
        assertThat(snapshots.map { it.message }).containsExactly("message-A", "message-B")
    }

    @ParameterizedTest(name = "{displayName} / {0}")
    @EnumSource(Backend::class)
    fun `conflicting add revalidates and consumes no rejected snapshot`(backend: Backend) {
        val f = fixture(backend)
        val c = "ar" + UUID.randomUUID().toString().replace("-", "")
        f.store.createCatalog(c, "s3://hoglake-it/$c")
        f.store.createNamespace(c, "ns")
        val original = f.store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val ops = listOf(AlterOp.AddColumn(ColumnDef("x", ColType.LONG)))
        val out = race(backend, f, c, ops, ops, expectedPublications = 1)
        assertThat(out.failure).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(f.store.listSnapshots(c, original.readSnapshotId, 100).first).hasSize(1)
        assertThat(f.store.getTable(c, "ns", "t").columns.map { it.def.name }).containsExactly("id", "x")
    }

    @ParameterizedTest(name = "{displayName} / {0}")
    @EnumSource(Backend::class)
    fun `fixed read snapshot refuses DDL without another publication`(backend: Backend) {
        val f = fixture(backend)
        val c = "ar" + UUID.randomUUID().toString().replace("-", "")
        f.store.createCatalog(c, "s3://hoglake-it/$c")
        f.store.createNamespace(c, "ns")
        val original = f.store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val out =
            race(
                backend,
                f,
                c,
                listOf(AlterOp.AddColumn(ColumnDef("a", ColType.LONG))),
                listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))),
                readSnapshot = original.readSnapshotId,
                expectedPublications = 1,
            )
        assertThat(out.failure).isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        val error = out.failure as HoglakeException.DdlSinceReadSnapshot
        assertThat(error.tables).containsExactly("ns.t")
        assertThat(error.readSnapshot).isEqualTo(original.readSnapshotId)
        assertThat(f.store.listSnapshots(c, original.readSnapshotId, 100).first).hasSize(1)
    }
}
