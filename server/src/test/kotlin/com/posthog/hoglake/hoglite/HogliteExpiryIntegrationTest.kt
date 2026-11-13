package com.posthog.hoglake.hoglite

import com.posthog.hoglake.api.toDto
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.wireObjectMapper
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/** Deterministic guard races use the live engine and direct store construction. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteExpiryIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var client: HogliteClient
    private lateinit var store: HogliteCatalogStore
    private lateinit var pstore: HogliteCatalogStore
    private lateinit var proxy: HttpServer
    private var catalogCounter = 0
    private var hogPort = 0
    private lateinit var proxyClient: HogliteClient
    private lateinit var forward: HttpClient

    /** A proxy hook: matched on (method, path, body); runs [before], then forwards (or short-circuits). */
    class Hook(
        val match: (String, String, String) -> Boolean,
        var remaining: Int = 1,
        val shortCircuit: Int? = null,
        val before: () -> Unit = {},
        val after: () -> Unit = {},
        val shortCircuitBody: String = "{\"error\":\"probe short-circuit\"}",
    )

    private val hooks = CopyOnWriteArrayList<Hook>()

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
        hogPort = URI.create(engine.url()).port
        client = engine.client
        store = engine.store
        forward = HttpClient.newHttpClient()
        proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        proxy.executor = Executors.newCachedThreadPool()
        proxy.createContext("/") { ex ->
            try {
                val method = ex.requestMethod
                val path = ex.requestURI.toString()
                val body = ex.requestBody.readBytes()
                val bs = body.decodeToString()
                val hook =
                    synchronized(hooks) {
                        hooks.firstOrNull { it.remaining > 0 && it.match(method, path, bs) }?.also { it.remaining-- }
                    }
                hook?.before?.invoke()

                fun respond(
                    status: Int,
                    bytes: ByteArray,
                ) {
                    ex.responseHeaders.add("Content-Type", "application/json")
                    ex.sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
                    if (bytes.isNotEmpty()) ex.responseBody.use { it.write(bytes) } else ex.close()
                }
                if (hook?.shortCircuit != null) {
                    respond(hook.shortCircuit, hook.shortCircuitBody.toByteArray())
                    return@createContext
                }
                val req =
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:$hogPort$path"))
                        .header("Content-Type", "application/json")
                        .method(
                            method,
                            if (body.isEmpty()) {
                                HttpRequest.BodyPublishers.noBody()
                            } else {
                                HttpRequest.BodyPublishers.ofByteArray(
                                    body,
                                )
                            },
                        ).build()
                val resp = forward.send(req, HttpResponse.BodyHandlers.ofByteArray())
                hook?.after?.invoke()
                respond(resp.statusCode(), resp.body())
            } catch (e: Throwable) {
                ex.sendResponseHeaders(599, -1)
                ex.close()
            }
        }
        proxy.start()
        proxyClient = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}", sleeper = {})
        pstore = checkedStore(proxyClient)
    }

    @AfterAll
    fun stop() {
        if (::proxyClient.isInitialized) proxyClient.close()
        if (::proxy.isInitialized) {
            proxy.stop(0)
            (proxy.executor as java.util.concurrent.ExecutorService).shutdownNow()
        }
        if (::forward.isInitialized) forward.close()
        if (::engine.isInitialized) engine.close()
    }

    private fun freshCatalog(): String {
        val name = "race${catalogCounter++}"
        store.createCatalog(name, "s3://hoglake-it/$name")
        return name
    }

    private fun setup(): Pair<String, com.posthog.hoglake.model.TableInfo> {
        val c = freshCatalog()
        store.createNamespace(c, "ns")
        return c to store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
    }

    private fun q(
        c: String,
        sql: String,
        vararg a: Any?,
    ) = store.catalogOperation(c) { store.queryCatalog(c, sql, *a) }

    private fun n(
        c: String,
        sql: String,
        vararg a: Any?,
    ): Long = q(c, sql, *a).firstRow()!![0].asLong()

    private fun head(c: String) = store.getCatalog(c).headSnapshotId

    private fun appendReq(
        c: String,
        path: String,
        key: UUID? = UUID.randomUUID(),
        expected: UUID? = null,
        read: Long? = null,
        table: String = "t",
    ) = CommitRequest(
        readSnapshot = read,
        idempotencyKey = key,
        appends =
            listOf(
                TableAppend(
                    "ns",
                    table,
                    listOf(FileRegistration("s3://hoglake-it/$c/data/$path", 10, 100, 50)),
                    expectedTableUuid = expected,
                ),
            ),
    )

    private fun dvReq(
        c: String,
        fileId: Long,
        read: Long?,
        path: String,
        count: Long,
    ) = CommitRequest(
        readSnapshot = read,
        idempotencyKey = UUID.randomUUID(),
        deletes =
            listOf(
                TableDeletes(
                    "ns",
                    "t",
                    listOf(DeleteFileRegistration(fileId, "s3://hoglake-it/$c/data/$path", count, 10)),
                ),
            ),
    )

    /** A write transaction: a commit's planning reads go out as a read-only `/tx` too, and are not one. */
    private fun isTx(
        m: String,
        p: String,
        b: String,
    ) = m == "POST" && p.endsWith("/tx") && !isReadBatch(b)

    /** The client marks read-only transactions with their txid prefix. */
    private fun isReadBatch(body: String) = "\"txid\":\"${HogliteClient.READ_TXID_PREFIX}" in body

    @Test
    fun `expiry respects retention consumer floor and typed expired reads and writes`() {
        val (c, t) = setup()
        val pin = store.commit(c, appendReq(c, "first.parquet")).snapshotId
        val head = store.commit(c, appendReq(c, "second.parquet")).snapshotId
        val expiry = HogliteExpiry(store, 100, 10_000)
        assertThat(expiry.runOnce(c, 100).newEarliestSnapshotId).isZero()
        store.commitOffset(c, "reader", t.tableUuid, pin)
        shortRetention(c)
        ageSnapshots(c)
        val pinned = expiry.runOnce(c, 100)
        assertThat(pinned.newEarliestSnapshotId).isEqualTo(pin)
        assertThat(pinned.flooredByConsumer).isEqualTo("reader")
        assertThat(store.planScan(c, "ns", "t", snapshot = pin)).hasSize(1)
        assertThatThrownBy { store.planScan(c, "ns", "t", snapshot = pin - 1) }
            .isExactlyInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy { store.commit(c, appendReq(c, "stale.parquet", read = pin - 1)) }
            .isExactlyInstanceOf(HoglakeException.Expired::class.java)
        store.options.patch(c, PatchField.Absent, false)
        assertThat(expiry.runOnce(c, 100).newEarliestSnapshotId).isEqualTo(head)
        assertThat(store.planScan(c, "ns", "t")).hasSize(2)
    }

    @Test
    fun `retired partition specs remain usable and never reuse their ids after expiry`() {
        val c = freshCatalog()
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.alters.alterTable(
            c,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        val old =
            FileRegistration(
                "s3://hoglake-it/$c/data/old.parquet",
                10,
                100,
                partitionValues = listOf("4"),
            )
        store.commit(c, CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(old)))))
        val retiredAt =
            store.alters.alterTable(c, "ns", "t", listOf(AlterOp.SetPartitionSpec(emptyList())))
                .readSnapshotId
        store.commit(c, appendReq(c, "new.parquet"))
        shortRetention(c)
        ageSnapshots(c)
        HogliteExpiry(store, 100, 10_000).runOnce(c, 100)
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(retiredAt)
        assertThat(HoglitePartitionLookup(store).lookup(c, "ns", "t", 1, listOf("4")).files.map { it.path })
            .containsExactly(old.path)
        val next =
            store.alters.alterTable(
                c,
                "ns",
                "t",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
            )
        assertThat(next.partitionSpec!!.specId).isEqualTo(2)
    }

    @Test
    fun `a failed floor advance still purges prior backlog and records its partial result`() {
        val (c, t) = setup()
        store.commit(c, appendReq(c, "ended.parquet"))
        store.truncateTable(c, "ns", "t", t.tableUuid, "test", "end history")
        shortRetention(c)
        ageSnapshots(c)
        val first = HogliteExpiry(store, 1, 0).runOnce(c, 100)
        assertThat(first.purgeTruncated).isTrue()
        assertThat(first.purgeRemaining).isEqualTo(1)
        store.options.patch(c, PatchField.Set(Long.MAX_VALUE), null)
        val failure = runCatching { HogliteExpiry(store, 1, 10_000).runOnce(c, 100) }.exceptionOrNull()
        assertThat(failure).isInstanceOf(PartialResult::class.java)
        assertThat(failure!!.cause).isInstanceOf(java.time.DateTimeException::class.java)
        val partial = (failure as PartialResult).partial as ExpiryResult
        assertThat(partial.dataFilesPurged).isEqualTo(1)
        assertThat(partial.newEarliestSnapshotId).isEqualTo(first.newEarliestSnapshotId)
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isZero()
        val run = q(c, "SELECT status, result_json FROM maintenance_runs ORDER BY id DESC LIMIT 1").firstRow()!!
        assertThat(run[0].asString()).isEqualTo("failed")
        assertThat(store.json.readTree(run[1].asString())["data_files_purged"].asLong()).isEqualTo(1)
    }

    @Test
    fun `an expiry sweep and its run ledger stay on their entry incarnation`() {
        val c = freshCatalog()
        var tenant = HogliteConformanceEngine.TENANTS.tenantFor(c)
        HogliteInterposer(engine.url()).use { proxy ->
            val routed = HogliteCatalogStore(proxy.client, HogliteTenantResolver { tenant })
            val hook =
                proxy.hook({ m, p, b ->
                    HogliteInterposer.isWriteTx(m, p, b) && "expiry-release" in b
                }) {
                    store.dropCatalog(c, "test", "replace")
                    tenant = "replacement-${UUID.randomUUID()}"
                    HogliteCatalogStore(engine.client, HogliteTenantResolver { tenant })
                        .createCatalog(c, "s3://hoglake-it/$c")
                }
            assertThatThrownBy { HogliteExpiry(routed, 100, 10_000).runOnce(c, 100) }
                .isInstanceOf(HoglakeException.NotFound::class.java)
            assertThat(hook.fired).isTrue()
            assertThat(engine.client.query(tenant, "SELECT COUNT(*) FROM maintenance_runs").firstRow()!![0].asLong())
                .isZero()
            assertThat(engine.client.query(tenant, "SELECT earliest_snapshot_id FROM meta").firstRow()!![0].asLong())
                .isZero()
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["data", "dv"])
    fun `concurrent sweeps queue only rows their purge page actually deletes`(arm: String) {
        val (c, t) = setup()
        val appended = store.commit(c, appendReq(c, "ended.parquet")).snapshotId
        val file = store.listFiles(c, "ns", "t").single()
        if (arm == "data") {
            store.truncateTable(c, "ns", "t", t.tableUuid, "test", "end history")
        } else {
            val first = store.commit(c, dvReq(c, file.dataFileId, appended, "old.puffin", 1)).snapshotId
            store.commit(c, dvReq(c, file.dataFileId, first, "live.puffin", 2))
        }
        shortRetention(c)
        ageSnapshots(c)
        val hook =
            Hook({ m, p, b -> isTx(m, p, b) && "expiry-purge-$arm" in b }, before = {
                HogliteExpiry(store, 100, 10_000).runOnce(c, 100)
            })
        hooks += hook
        val result = HogliteExpiry(pstore, 100, 10_000).runOnce(c, 100)
        assertThat(hook.remaining).isZero()
        assertThat(result.dataFilesPurged + result.deleteFilesQueued).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM removals")).isEqualTo(1)
    }

    @Test
    fun `a failed purge page rolls back its queue and the next sweep drains it with retention disabled`() {
        val (c, t) = setup()
        val appended = store.commit(c, appendReq(c, "ended.parquet")).snapshotId
        val file = store.listFiles(c, "ns", "t").single()
        store.commit(c, dvReq(c, file.dataFileId, appended, "riding.puffin", 1))
        store.truncateTable(c, "ns", "t", t.tableUuid, "test", "end history")
        shortRetention(c)
        ageSnapshots(c)
        // Exercise the data arm with a vector whose lifetime follows its ended data file.
        edit(c, "UPDATE dvs SET end_snapshot = NULL")
        val hook =
            Hook(
                { m, p, b -> isTx(m, p, b) && "expiry-purge-data" in b },
                shortCircuit = 400,
            )
        hooks += hook
        val failed = HogliteExpiry(pstore, 1, 10_000).runOnce(c, 100)
        assertThat(hook.remaining).isZero()
        assertThat(failed.purgeFailures).isEqualTo(1)
        assertThat(failed.purgeTruncated).isTrue()
        assertThat(failed.purgeRemaining).isEqualTo(1)
        assertThat(n(c, "SELECT COUNT(*) FROM removals")).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
        assertThat(n(c, "SELECT COUNT(*) FROM dvs")).isEqualTo(1)
        store.options.patch(c, PatchField.Set(null), null)
        val drained = HogliteExpiry(store, 1, 10_000).runOnce(c, 100)
        assertThat(drained.snapshotsExpired).isZero()
        assertThat(drained.dataFilesQueued).isEqualTo(1)
        assertThat(drained.deleteFilesQueued).isEqualTo(1)
        assertThat(drained.purgeFailures).isZero()
        assertThat(drained.purgeTruncated).isFalse()
        assertThat(n(c, "SELECT COUNT(*) FROM removals")).isEqualTo(2)
    }

    /** Age every snapshot far past any retention cutoff, deterministically (no sleeps). */
    private fun edit(
        c: String,
        sql: String,
        vararg args: Any?,
    ) = boundTx(c, HogliteCatalogStore.freshTxid("replay-fixture"), null, HogliteTx().apply { add(sql, *args) })

    private fun prepareCreation(
        c: String,
        name: String,
        replacement: com.posthog.hoglake.service.ReplacementTarget?,
    ): Pair<UUID, com.posthog.hoglake.service.TableCreation> {
        val op = UUID.randomUUID()
        val prepared =
            store.tableCreations.prepare(
                c,
                op,
                com.posthog.hoglake.service.TableCreationDefinition(
                    "ns",
                    name,
                    listOf(ColumnDef("id", ColType.LONG)),
                    replacement = replacement,
                    author = "test",
                    message = "replace",
                ),
            )
        return op to prepared
    }

    /** Raw fixture SQL still enters an explicit scope; never exempt production work. */
    private fun checkedStore(client: HogliteClient) = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)

    private fun boundTx(
        c: String,
        txid: String,
        fp: String?,
        tx: HogliteTx,
    ) = store.catalogOperation(c) { store.runTx(c, txid, fp, tx) }

    /** Age every snapshot far past any retention cutoff, deterministically (no sleeps). */
    private fun ageSnapshots(c: String) {
        boundTx(
            c,
            HogliteCatalogStore.freshTxid("age"),
            null,
            HogliteTx().apply { add("UPDATE snapshots SET ts = ? WHERE id >= 0", "2001-01-01T00:00:00.000Z") },
        )
    }

    private fun shortRetention(c: String) {
        HogliteOptions(
            store,
        ).patch(c, PatchField.Set(1L), consumerFloor = null, author = "test", message = "shorten retention")
    }

    private fun expireAfterChangesMeta(c: String): Hook =
        Hook({ m, p, b -> m == "POST" && p.endsWith("/query") && store.metaSql in b }, after = {
            HogliteExpiry(store, purgePage = 100, purgeBudgetMs = 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
        }).also { hooks += it }

    @Test
    fun `a concurrent sweep cannot move the floor backwards`() {
        val (c, _) = setup()
        repeat(6) { store.commit(c, appendReq(c, "e$it.parquet")) }
        shortRetention(c)
        ageSnapshots(c)
        val pre = store.getCatalog(c).earliestSnapshotId
        // Between the slow sweep's plan and its advance tx, a fast sweep runs to completion.
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
                HogliteExpiry(store, purgePage = 100, purgeBudgetMs = 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
            })
        val r =
            runCatching {
                HogliteExpiry(
                    pstore,
                    purgePage = 100,
                    purgeBudgetMs = 10_000,
                ).runOnce(c, 1, MaintenanceTrigger.MANUAL)
            }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(pre + 1)
    }

    @Test
    fun `a sweep planned under a since-disabled retention policy refuses to publish`() {
        val (c, _) = setup()
        repeat(3) { store.commit(c, appendReq(c, "r$it.parquet")) }
        shortRetention(c)
        ageSnapshots(c)
        val snapshotsBefore = n(c, "SELECT COUNT(*) FROM snapshots")
        // Disable retention between the sweep's plan and its advance tx.
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
                HogliteOptions(store)
                    .patch(
                        c,
                        PatchField.Set(null),
                        consumerFloor = null,
                        author = "test",
                        message = "disable retention",
                    )
            })
        val r =
            runCatching {
                HogliteExpiry(
                    pstore,
                    purgePage = 100,
                    purgeBudgetMs = 10_000,
                ).runOnce(c, 100, MaintenanceTrigger.MANUAL)
            }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(n(c, "SELECT COUNT(*) FROM snapshots")).isEqualTo(snapshotsBefore)
        assertThat(store.getCatalog(c).earliestSnapshotId).isEqualTo(0L)
    }

    @Test
    fun `a sweep planned with consumer protection disabled refuses once it is enabled`() {
        val (c, t) = setup()
        repeat(3) { store.commit(c, appendReq(c, "cf$it.parquet")) }
        shortRetention(c)
        // A consumer holds snapshot 1; protection is OFF while the sweep plans.
        HogliteOptions(
            store,
        ).patch(c, PatchField.Set(1L), consumerFloor = false, author = "test", message = "protection off")
        store.commitOffset(c, "consumer-1", t.tableUuid, 1)
        ageSnapshots(c)
        // Protection turns on between the plan and the advance tx.
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
                HogliteOptions(store)
                    .patch(c, PatchField.Set(1L), consumerFloor = true, author = "test", message = "protection on")
            })
        val r =
            runCatching {
                HogliteExpiry(
                    pstore,
                    purgePage = 100,
                    purgeBudgetMs = 10_000,
                ).runOnce(c, 100, MaintenanceTrigger.MANUAL)
            }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(store.getCatalog(c).earliestSnapshotId).isEqualTo(0L)
        // And a fresh sweep, planned WITH protection, stops at the consumer.
        HogliteExpiry(store, purgePage = 100, purgeBudgetMs = 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
        assertThat(store.getCatalog(c).earliestSnapshotId).isEqualTo(1L)
    }

    @Test
    fun `an offset commit re-checks the expiry floor inside its transaction`() {
        val (c, t) = setup()
        repeat(3) { store.commit(c, appendReq(c, "o$it.parquet")) }
        shortRetention(c)
        ageSnapshots(c)
        // Between the offset's preflight and its tx, a sweep advances the
        // floor past the offset's snapshot.
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "INSERT INTO offsets" in b }, before = {
                HogliteExpiry(store, purgePage = 100, purgeBudgetMs = 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
            })
        assertThatThrownBy { pstore.commitOffset(c, "consumer-1", t.tableUuid, 1) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        // The sweep really did advance the floor past the refused offset.
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(1L)
        assertThatThrownBy { store.getOffset(c, "consumer-1", t.tableUuid) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `a changefeed refuses expiry that purges its truncate barrier after preflight`() {
        val (c, t) = setup()
        store.commit(c, appendReq(c, "before.parquet"))
        val from = head(c)
        val truncated = store.truncateTable(c, "ns", "t", t.tableUuid, "test", "truncate").snapshotId
        store.commit(c, appendReq(c, "after.parquet"))
        shortRetention(c)
        ageSnapshots(c)
        val to = head(c)
        assertThatThrownBy { store.changes(c, "ns", "t", from, to) }
            .isInstanceOf(HoglakeException.ReconciliationRequired::class.java)

        val hook = expireAfterChangesMeta(c)
        assertThatThrownBy { pstore.changes(c, "ns", "t", from, to) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isEqualTo(to)
        assertThat(n(c, "SELECT COUNT(*) FROM changes WHERE snapshot_id = ?", truncated)).isZero()
        // The incomplete feed would otherwise let the consumer advance beyond the lost barrier.
        store.commitOffset(c, "consumer-1", t.tableUuid, to)
        assertThat(store.getOffset(c, "consumer-1", t.tableUuid).committedSnapshot).isEqualTo(to)
    }

    @Test
    fun `a changefeed refuses expiry that purges ended files and vectors after preflight`() {
        val (c, t) = setup()
        val from = head(c)
        val appended = store.commit(c, appendReq(c, "ended.parquet")).snapshotId
        val file = store.listFiles(c, "ns", "t").single()
        val first = store.commit(c, dvReq(c, file.dataFileId, appended, "first.puffin", 1)).snapshotId
        val to = store.commit(c, dvReq(c, file.dataFileId, first, "second.puffin", 2)).snapshotId
        // The truncate is outside the requested window. Only file/DV history represents this window.
        store.truncateTable(c, "ns", "t", t.tableUuid, "test", "end history")
        shortRetention(c)
        ageSnapshots(c)
        val retained = store.changes(c, "ns", "t", from, to)
        assertThat(retained.files.map { it.dataFileId }).containsExactly(file.dataFileId)
        assertThat(retained.deleteFiles.map { it.path }).containsExactly(
            "s3://hoglake-it/$c/data/first.puffin",
            "s3://hoglake-it/$c/data/second.puffin",
        )

        val hook = expireAfterChangesMeta(c)
        assertThatThrownBy { pstore.changes(c, "ns", "t", from, to) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(to)
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM dvs")).isZero()
    }

    @Test
    fun `a changefeed allows floor minus one after concurrent expiry and rejects an earlier bound`() {
        val (c, _) = setup()
        shortRetention(c)
        val appended = store.commit(c, appendReq(c, "older.parquet")).snapshotId
        val file = store.listFiles(c, "ns", "t").single()
        val first = store.commit(c, dvReq(c, file.dataFileId, appended, "older.puffin", 1)).snapshotId
        val to =
            store.commit(
                c,
                dvReq(c, file.dataFileId, first, "retained.puffin", 2)
                    .copy(appends = appendReq(c, "retained.parquet").appends),
            ).snapshotId
        // The superseded vector ends exactly at the future floor. Its insertion is at floor minus one.
        assertThat(n(c, "SELECT end_snapshot FROM dvs WHERE path LIKE '%older.puffin'")).isEqualTo(to)
        assertThat(n(c, "SELECT begin_snapshot FROM dvs WHERE path LIKE '%older.puffin'")).isEqualTo(to - 1)
        ageSnapshots(c)
        val hook = expireAfterChangesMeta(c)
        val plan = pstore.changes(c, "ns", "t", to - 1)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isEqualTo(to)
        assertThat(plan.toSnapshot).isEqualTo(to)
        assertThat(plan.files.map { it.path }).containsExactly("s3://hoglake-it/$c/data/retained.parquet")
        assertThat(plan.deleteFiles.map { it.path }).containsExactly("s3://hoglake-it/$c/data/retained.puffin")
        assertThat(n(c, "SELECT COUNT(*) FROM dvs WHERE path LIKE '%older.puffin'")).isZero()
        assertThatThrownBy { pstore.changes(c, "ns", "t", to - 2) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        val boundary = pstore.changes(c, "ns", "t", to - 1, to - 1)
        assertThat(boundary.files).isEmpty()
        assertThat(boundary.deleteFiles).isEmpty()
        val empty = pstore.changes(c, "ns", "t", to, to)
        assertThat(empty.files).isEmpty()
        assertThat(empty.deleteFiles).isEmpty()
    }

    @Test
    fun `a consumer registered below a protected sweep's planned floor keeps its history`() {
        val (c, t) = setup()
        repeat(3) { store.commit(c, appendReq(c, "consumer$it.parquet")) }
        HogliteOptions(store).patch(c, PatchField.Set(1L), consumerFloor = true, author = "test", message = "protect")
        ageSnapshots(c)
        val hook =
            Hook({ m, p, b -> isTx(m, p, b) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
                store.commitOffset(c, "late-consumer", t.tableUuid, 1)
            })
        hooks += hook
        assertThatThrownBy {
            HogliteExpiry(pstore, 100, 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM snapshots WHERE id = 1")).isEqualTo(1L)
    }

    @Test
    fun `a commit re-checks its read snapshot against the in-transaction expiry floor`() {
        val (c, _) = setup()
        repeat(3) { store.commit(c, appendReq(c, "floor$it.parquet")) }
        shortRetention(c)
        ageSnapshots(c)
        val hook =
            Hook({ m, p, b -> isTx(m, p, b) && "expired-read.parquet" in b }, before = {
                HogliteExpiry(store, 100, 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
            })
        hooks += hook
        assertThatThrownBy { pstore.commit(c, appendReq(c, "expired-read.parquet", read = 1)) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(1L)
        assertThat(n(c, "SELECT COUNT(*) FROM files WHERE path LIKE '%expired-read.parquet'")).isZero()
    }

    @Test
    fun `an upload renewed after sweep planning is not queued for removal`() {
        val c = freshCatalog()
        val owner = UUID.randomUUID()
        val claim = store.uploads.claim(c, UUID.randomUUID(), owner, "s3://hoglake-it/$c/up", "data")
        boundTx(
            c,
            HogliteCatalogStore.freshTxid("age-upload"),
            null,
            HogliteTx().apply {
                add("UPDATE uploads SET expires_at = '2001-01-01T00:00:00.000Z'")
            },
        )
        val hook =
            Hook({ m, p, b -> isTx(m, p, b) && "UPDATE uploads SET state = 'abandoned', scheduled_at" in b }, before = {
                assertThat(store.uploads.renew(c, owner)).isEqualTo(1)
            })
        hooks += hook
        assertThat(pstore.uploads.scheduleExpired(c, 100)).isZero()
        assertThat(hook.remaining).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM removals WHERE path = ?", claim.path)).isZero()
        assertThat(q(c, "SELECT state FROM uploads WHERE path = ?", claim.path).firstRow()!![0]).isEqualTo("active")
    }

    @Test
    fun `a replacement re-checks its read snapshot against the in-transaction expiry floor`() {
        val (c, t) = setup()
        store.createTable(c, "ns", "other", listOf(ColumnDef("id", ColType.LONG)))
        store.commit(c, appendReq(c, "before.parquet"))
        shortRetention(c)
        val read = head(c)
        val (op, _) = prepareCreation(c, "t", com.posthog.hoglake.service.ReplacementTarget(t.tableUuid, read))
        // After the preflight saw no change to the target since `read`: an
        // acknowledged append lands on it, another commit moves head past
        // it, and expiry advances the floor beyond `read`, purging the
        // append's change row. The changes guard is then vacuous.
        val hook =
            Hook({ m, p, b -> m == "POST" && p.endsWith("/query") && "kind <> 'table_compacted'" in b }, after = {
                store.commit(c, appendReq(c, "acknowledged.parquet"))
                store.commit(c, appendReq(c, "elsewhere.parquet", table = "other"))
                ageSnapshots(c)
                HogliteExpiry(store, 100, 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
            })
        hooks += hook
        val published = pstore.tableCreations.publish(c, op, emptyList())
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(read)
        assertThat(published.state).isEqualTo("rejected")
        assertThat(published.reason).isEqualTo("target_changed")
        // The target, with the acknowledged append, is still the live table.
        assertThat(store.getTable(c, "ns", "t").tableUuid).isEqualTo(t.tableUuid)
        assertThat(store.listFiles(c, "ns", "t").map { it.path })
            .contains("s3://hoglake-it/$c/data/acknowledged.parquet")
    }

    private fun assertEndedVersions(
        cat: String,
        id: Long,
        renamed: Long,
        at: Long,
    ) {
        val rows = q(cat, "SELECT end_snapshot FROM columns WHERE table_id = ? ORDER BY begin_snapshot", id).rows
        assertThat(rows.map { it[0].asLongOrNull() }).containsExactly(renamed, at)
        assertThat(
            q(cat, "SELECT field_id FROM columns WHERE table_id = ? AND end_snapshot IS NULL", id).rows,
        ).isEmpty()
    }

    @Test
    fun `drop ends columns at its actual snapshot and expiry reclaims churn`() {
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val survivor = store.createTable(cat, "ns", "survivor", listOf(ColumnDef("kept", ColType.LONG)))
        repeat(12) { i ->
            val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            val renamed =
                HogliteAlters(store).alterTable(
                    cat,
                    "ns",
                    "t",
                    listOf(com.posthog.hoglake.model.AlterOp.RenameColumn("id", "renamed")),
                ).readSnapshotId
            store.createNamespace(cat, "gap$i") // The drop must use its allocated snapshot, not a pre-read head.
            val before = store.getCatalog(cat).headSnapshotId
            val dropped = store.dropTable(cat, "ns", "t", table.tableUuid, "test", "drop").snapshotId
            assertThat(dropped).isGreaterThan(before)
            assertThat(
                store.getTable(cat, "ns", "t", snapshot = before).columns.map { it.def.name },
            ).containsExactly("renamed")
            assertThatThrownBy { store.getTable(cat, "ns", "t", snapshot = dropped) }
                .isInstanceOf(com.posthog.hoglake.model.HoglakeException.NotFound::class.java)
            assertEndedVersions(cat, table.tableId, renamed, dropped)
        }
        expireAll(cat)
        assertThat(q(cat, "SELECT table_id FROM columns").rows.map { it[0].asLong() })
            .containsExactly(survivor.tableId)
        assertThat(store.getTable(cat, "ns", "survivor").columns.map { it.def.name }).containsExactly("kept")
    }

    @Test
    fun `replacement ends only old columns at the publication snapshot and expiry reclaims churn`() {
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val survivor = store.createTable(cat, "ns", "survivor", listOf(ColumnDef("kept", ColType.LONG)))
        var current = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        repeat(12) { i ->
            val renamed =
                HogliteAlters(store).alterTable(
                    cat,
                    "ns",
                    "t",
                    listOf(
                        com.posthog.hoglake.model.AlterOp.RenameColumn(current.columns.single().def.name, "renamed$i"),
                    ),
                ).readSnapshotId
            val before = store.getCatalog(cat).headSnapshotId
            val op = UUID.randomUUID()
            store.tableCreations.prepare(
                cat,
                op,
                com.posthog.hoglake.service.TableCreationDefinition(
                    "ns",
                    "t",
                    listOf(ColumnDef("new$i", ColType.LONG)),
                    replacement = com.posthog.hoglake.service.ReplacementTarget(current.tableUuid, before),
                    author = "test",
                    message = "replace",
                ),
            )
            store.createNamespace(cat, "gap$i")
            val published = store.tableCreations.publish(cat, op, emptyList())
            assertThat(published.state).isEqualTo("committed")
            val at = published.snapshotId!!
            assertThat(at).isGreaterThan(before)
            assertThat(store.getTable(cat, "ns", "t", snapshot = before).tableUuid).isEqualTo(current.tableUuid)
            assertEndedVersions(cat, current.tableId, renamed, at)
            current = store.getTable(cat, "ns", "t", snapshot = at)
            assertThat(current.columns.map { it.def.name }).containsExactly("new$i")
            assertThat(
                q(cat, "SELECT end_snapshot FROM columns WHERE table_id = ?", current.tableId).rows.single()[0],
            ).isNull()
        }
        expireAll(cat)
        assertThat(
            q(cat, "SELECT table_id FROM columns").rows.map { it[0].asLong() },
        ).containsExactlyInAnyOrder(survivor.tableId, current.tableId)
        assertThat(store.getTable(cat, "ns", "survivor").columns.map { it.def.name }).containsExactly("kept")
    }

    private fun expireAll(cat: String) {
        // Backdate real snapshots, then drive actual expiry without wall-clock sleeps.
        edit(cat, "UPDATE meta SET snapshot_retention_seconds = 1 WHERE id = 1")
        edit(cat, "UPDATE snapshots SET ts = '2000-01-01T00:00:00.000Z'")
        val result = store.catalogOperation(cat) { HogliteExpiry(store, 100, 10000).runOnce(cat, 1000) }
        assertThat(result.newEarliestSnapshotId).isEqualTo(store.getCatalog(cat).headSnapshotId)
    }

    @Test
    fun `superseded offsets release with retention disabled but unreconciled consumers stay`() {
        val (c, old) = setup()
        val pin = head(c)
        store.commitOffset(c, "reconciled", old.tableUuid, pin)
        store.commitOffset(c, "unreconciled", old.tableUuid, pin)
        val (op, _) = prepareCreation(c, "t", com.posthog.hoglake.service.ReplacementTarget(old.tableUuid, pin))
        assertThat(store.tableCreations.publish(c, op, emptyList()).state).isEqualTo("committed")
        val replacement = store.getTable(c, "ns", "t")
        store.commitOffset(c, "reconciled", replacement.tableUuid, head(c))
        // Restore a row stranded before the commit path learned to release its ancestors.
        edit(
            c,
            "INSERT INTO offsets(consumer_id, table_uuid, committed_snapshot, updated_at) VALUES (?, ?, ?, ?)",
            "reconciled",
            old.tableUuid.toString(),
            pin,
            HogliteSchema.now(),
        )
        val result = HogliteExpiry(store, 100, 10_000).runOnce(c, 100)
        assertThat(result.snapshotsExpired).isZero()
        assertThat(result.newEarliestSnapshotId).isZero()
        assertThatThrownBy { store.getOffset(c, "reconciled", old.tableUuid) }
            .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(result.offsetsReleased).isEqualTo(1)
        assertThat(store.getOffset(c, "unreconciled", old.tableUuid).committedSnapshot).isEqualTo(pin)
        assertThat(store.getOffset(c, "reconciled", replacement.tableUuid).committedSnapshot).isEqualTo(head(c))
    }

    @ParameterizedTest
    @ValueSource(strings = ["pinned", "head", "files", "scan", "stats", "listing", "partition-values"])
    fun `metadata reads refuse expiry after their initial floor check`(mode: String) {
        val (c, _) = setup()
        val sort =
            AlterOp.SetSortOrder(
                listOf(
                    com.posthog.hoglake.model.SortFieldDef(
                        1,
                        com.posthog.hoglake.model.SortDirection.ASC,
                        com.posthog.hoglake.model.NullOrder.NULLS_LAST,
                    ),
                ),
            )
        store.alters.alterTable(c, "ns", "t", listOf(sort))
        val req = appendReq(c, "retained.parquet")
        val append = req.appends.single()
        store.commit(
            c,
            req.copy(
                appends =
                    listOf(
                        append.copy(
                            files =
                                listOf(
                                    append.files.single().copy(
                                        columnStats =
                                            listOf(
                                                com.posthog.hoglake.model.ColumnStats(1, 10, 0, null, null, null, null),
                                            ),
                                    ),
                                ),
                        ),
                    ),
            ),
        )
        val at = head(c)
        val file = store.listFiles(c, "ns", "t").single()
        shortRetention(c)
        val marker =
            when (mode) {
                "files" -> "FROM sort_specs WHERE"
                "listing" -> "SELECT t.id, t.table_uuid, v.name"
                "partition-values" -> "FROM partition_specs WHERE"
                else -> "FROM columns WHERE"
            }
        val hook =
            Hook({ m, p, b -> m == "POST" && p.endsWith("/query") && marker in b }, before = {
                store.alters.alterTable(c, "ns", "t", listOf(AlterOp.RenameColumn("id", "renamed")))
                store.commit(c, appendReq(c, "new-head.parquet"))
                ageSnapshots(c)
                HogliteExpiry(store, 100, 10_000).runOnce(c, 100)
            })
        hooks += hook
        assertThatThrownBy {
            when (mode) {
                "pinned" -> pstore.getTable(c, "ns", "t", snapshot = at)
                "head" -> pstore.getTable(c, "ns", "t")
                "files" -> pstore.listFiles(c, "ns", "t", snapshot = at)
                "scan" ->
                    pstore.planScan(
                        c,
                        "ns",
                        "t",
                        snapshot = at,
                        columnStats = com.posthog.hoglake.service.ScanService.ColumnStatsRequest(),
                    )
                "stats" -> pstore.fileStats(c, "ns", "t", file.dataFileId, snapshot = at)
                "listing" -> pstore.listTables(c, "ns", snapshot = at)
                else -> pstore.partitionValues(c, "ns", "t", snapshot = at)
            }
        }.isExactlyInstanceOf(HoglakeException.Expired::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(at)
    }

    @ParameterizedTest
    @ValueSource(strings = ["head", "pinned", "expired"])
    fun `expiry cannot change a sort allocator between an alter plan and its receipt`(mode: String) {
        val (c, _) = setup()
        val sort =
            AlterOp.SetSortOrder(
                listOf(
                    com.posthog.hoglake.model.SortFieldDef(
                        1,
                        com.posthog.hoglake.model.SortDirection.ASC,
                        com.posthog.hoglake.model.NullOrder.NULLS_LAST,
                    ),
                ),
            )
        store.alters.alterTable(c, "ns", "t", listOf(sort))
        store.alters.alterTable(c, "ns", "t", listOf(AlterOp.SetSortOrder(emptyList())))
        val at = head(c)
        shortRetention(c)
        ageSnapshots(c)
        val hook =
            Hook({ m, p, b -> isTx(m, p, b) && "INSERT INTO sort_specs" in b }, before = {
                if (mode == "expired") {
                    store.commit(c, appendReq(c, "advance-floor.parquet"))
                    ageSnapshots(c)
                }
                HogliteExpiry(store, 100, 10_000).runOnce(c, 100)
            })
        hooks += hook
        val outcome =
            runCatching {
                pstore.alters.alterTable(c, "ns", "t", listOf(sort), readSnapshot = at.takeUnless { mode == "head" })
            }
        assertThat(hook.remaining).isZero()
        if (mode == "expired") {
            assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(at)
            assertThat(outcome.exceptionOrNull()).isExactlyInstanceOf(HoglakeException.Expired::class.java)
        } else if (mode == "pinned") {
            assertThat(outcome.exceptionOrNull()).isInstanceOf(HoglakeException.CommitConflict::class.java)
            assertThat(n(c, "SELECT COUNT(*) FROM sort_specs WHERE end_snapshot IS NULL")).isZero()
        } else {
            val receipt = outcome.getOrThrow()
            val persisted = store.getTable(c, "ns", "t").sortSpec!!
            assertThat(receipt.sortSpec).isEqualTo(persisted)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["floor", "count"])
    fun `failed purge probes preserve an unknown backlog in the result and ledger`(probe: String) {
        val (c, t) = setup()
        store.commit(c, appendReq(c, "ended.parquet"))
        store.truncateTable(c, "ns", "t", t.tableUuid, "test", "end history")
        shortRetention(c)
        ageSnapshots(c)
        HogliteExpiry(store, 1, 0).runOnce(c, 100)
        store.options.patch(c, PatchField.Set(null), null)
        var metadataReads = 0
        val hook =
            Hook({ m, p, b ->
                m == "POST" && p.endsWith("/query") &&
                    if (probe == "count") {
                        "SELECT (SELECT COUNT(*) FROM (SELECT 1 FROM files" in b
                    } else {
                        store.metaSql in b && ++metadataReads == 2
                    }
            }, shortCircuit = 400)
        hooks += hook
        val result = HogliteExpiry(pstore, 1, 0).runOnce(c, 100)
        assertThat(hook.remaining).isZero()
        assertThat(result.purgeTruncated).isTrue()
        assertThat(result.purgeRemaining).isNull()
        val body = wireObjectMapper().readTree(wireObjectMapper().writeValueAsString(result.toDto()))
        assertThat(body.has("purge_remaining")).isFalse()
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
        val run = q(c, "SELECT result_json FROM maintenance_runs ORDER BY id DESC LIMIT 1").firstRow()!!
        val json = store.json.readTree(run[0].asString())
        assertThat(json["purge_remaining"]?.isNull != false).isTrue()
    }
}
