package com.posthog.hoglake.hoglite

import com.posthog.hoglake.commit.commitFingerprint
import com.posthog.hoglake.commit.commitFingerprintDigest
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.wireObjectMapper
import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/** Deterministic guard races use the live engine and direct store construction. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteExtendedGuardRaceIntegrationTest {
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
        val response: ((Int, ByteArray) -> Pair<Int, ByteArray>)? = null,
        val shortCircuitBody: String = "{\"error\":\"probe short-circuit\"}",
        val dropResponse: () -> Boolean = { false },
    )

    private val hooks = CopyOnWriteArrayList<Hook>()
    private val seen = ConcurrentLinkedQueue<String>()

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
                seen += "$method $path $bs"
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
                val answer =
                    hook?.response?.invoke(resp.statusCode(), resp.body()) ?: (resp.statusCode() to resp.body())
                if (hook?.dropResponse?.invoke() == true) {
                    // The engine applies the write before the proxy closes the connection.
                    ex.close()
                    return@createContext
                }
                respond(answer.first, answer.second)
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

    /** A read-only `/tx` (body or `seen` line): the client marks its txid. */
    private fun isReadBatch(body: String) = "\"txid\":\"${HogliteClient.READ_TXID_PREFIX}" in body

    /** The `seen` line of a write transaction carrying [marker]. */
    private fun isWriteTxLine(
        line: String,
        marker: String,
    ) = line.startsWith("POST") && "/tx " in line && !isReadBatch(line) && marker in line

    /**
     * A commit attempt's planning read on [tenant] whose statements include
     * [marker]: one read-only `/tx` carrying every pre-flight read.
     */
    private fun isPlanRead(
        p: String,
        b: String,
        tenant: String,
        marker: String,
    ) = "/v1/t/$tenant/tx" in p && isReadBatch(b) && marker in b

    /** Age every snapshot far past any retention cutoff, deterministically (no sleeps). */
    private fun digest(request: CommitRequest) =
        commitFingerprintDigest(commitFingerprint(request)).joinToString("") { "%02x".format(it) }

    private fun edit(
        c: String,
        sql: String,
        vararg args: Any?,
    ) = boundTx(c, HogliteCatalogStore.freshTxid("replay-fixture"), null, HogliteTx().apply { add(sql, *args) })

    private fun receiptRead(
        m: String,
        p: String,
        b: String,
        key: UUID,
    ) = m == "POST" && p.endsWith("/query") &&
        "SELECT fingerprint, snapshot_id, schema_version FROM receipts" in b && key.toString() in b

    private fun isAlterTx(
        m: String,
        p: String,
        b: String,
    ) = isTx(m, p, b) && "table_altered" in b

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

    @Test
    fun `a changefeed without expiry keeps the original head through concurrent appends`() {
        val (c, t) = setup()
        val from = head(c)
        val appended = store.commit(c, appendReq(c, "original.parquet")).snapshotId
        val file = store.listFiles(c, "ns", "t").single()
        val to = store.commit(c, dvReq(c, file.dataFileId, appended, "original.puffin", 1)).snapshotId
        val hook =
            Hook({ m, p, b -> m == "POST" && p.endsWith("/query") && store.metaSql in b }, after = {
                store.commit(c, appendReq(c, "later.parquet"))
            })
        hooks += hook
        val plan = pstore.changes(c, "ns", "t", from)
        assertThat(hook.remaining).isZero()
        assertThat(plan.tableUuid).isEqualTo(t.tableUuid)
        assertThat(plan.fromSnapshot).isEqualTo(from)
        assertThat(plan.toSnapshot).isEqualTo(to)
        assertThat(plan.files.map { it.dataFileId }).containsExactly(file.dataFileId)
        assertThat(plan.deleteFiles.map { it.path }).containsExactly("s3://hoglake-it/$c/data/original.puffin")
        assertThat(head(c)).isGreaterThan(to)
    }

    @Test
    fun `a schema fence replan reports DDL before dropped stats or stale partition values`() {
        for (partition in listOf(false, true)) {
            val (c, _) = setup()
            if (!partition) {
                store.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("extra", ColType.LONG))))
            }
            val read = head(c)
            val file =
                FileRegistration(
                    "s3://hoglake-it/$c/fenced.parquet",
                    10,
                    100,
                    50,
                    columnStats = if (partition) null else listOf(ColumnStats(2, 10, 0, null, null, null, null)),
                )
            val key = UUID.randomUUID()
            val beforeFileId = n(c, "SELECT next_file_id FROM meta")
            val hook =
                Hook({ m, p, b -> isTx(m, p, b) && "INSERT INTO files" in b && file.path in b }, before = {
                    val op =
                        if (partition) {
                            AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))
                        } else {
                            AlterOp.DropColumn("extra")
                        }
                    store.alters.alterTable(c, "ns", "t", listOf(op))
                })
            hooks += hook
            seen.clear()
            val error =
                runCatching {
                    pstore.commit(
                        c,
                        CommitRequest(
                            readSnapshot = read,
                            idempotencyKey = key,
                            appends = listOf(TableAppend("ns", "t", listOf(file))),
                        ),
                    )
                }.exceptionOrNull()
            assertThat(hook.remaining).isZero()
            assertThat(error).isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
            assertThat((error as HoglakeException.DdlSinceReadSnapshot).tables).containsExactly("ns.t")
            assertThat(error.readSnapshot).isEqualTo(read)
            assertThat(seen.count { isWriteTxLine(it, "fenced.parquet") }).isEqualTo(1)
            assertThat(seen.count { isReadBatch(it) && key.toString() in it && "FROM receipts" in it }).isEqualTo(2)
            assertThat(head(c)).isEqualTo(read + 1)
            assertThat(n(c, "SELECT next_file_id FROM meta")).isEqualTo(beforeFileId)
            assertThat(n(c, "SELECT next_row_id FROM tables WHERE dropped_snapshot IS NULL")).isZero()
            assertThat(n(c, "SELECT COUNT(*) FROM receipts WHERE idempotency_key = ?", key.toString())).isZero()
            assertThat(store.listFiles(c, "ns", "t")).isEmpty()
        }
    }

    @Test
    fun `duplicate paths roll back when a claim changes or a claimed removal appears after planning`() {
        for (mode in listOf("abandon", "owner", "removal")) {
            val (c, _) = setup()
            val owner = UUID.randomUUID()
            val claim = store.uploads.claim(c, UUID.randomUUID(), owner, "s3://hoglake-it/$c/up", "data")
            val file = FileRegistration(claim.path, 10, 100, 50)
            val beforeHead = head(c)
            val beforeFileId = n(c, "SELECT next_file_id FROM meta")
            val hook =
                Hook({ m, p, b -> isTx(m, p, b) && "INSERT INTO files" in b && claim.path in b }, before = {
                    if (mode == "removal") {
                        store.catalogOperation(c) {
                            store.runTx(
                                c,
                                HogliteCatalogStore.freshTxid("removal"),
                                null,
                                HogliteTx().apply {
                                    add(
                                        "INSERT INTO removals(path,file_kind,reason,queued_snapshot,queued_at," +
                                            "claimed_at,claimed_by) " +
                                            "VALUES (?,'data','snapshot_expiry',0,'2026-11-03','2026-11-03','worker')",
                                        claim.path,
                                    )
                                },
                            )
                        }
                    } else if (mode == "owner") {
                        store.catalogOperation(c) {
                            store.runTx(
                                c,
                                HogliteCatalogStore.freshTxid("owner"),
                                null,
                                HogliteTx().apply {
                                    add(
                                        "UPDATE uploads SET owner = ? WHERE path = ?",
                                        UUID.randomUUID().toString(),
                                        claim.path,
                                    )
                                },
                            )
                        }
                    } else {
                        store.uploads.abandon(c, owner, listOf(claim.path))
                    }
                })
            hooks += hook
            val error =
                runCatching {
                    pstore.commit(
                        c,
                        CommitRequest(
                            idempotencyKey = owner,
                            appends = listOf(TableAppend("ns", "t", listOf(file, file))),
                        ),
                    )
                }.exceptionOrNull()
            assertThat(hook.remaining).isZero()
            assertThat(error).isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            assertThat(head(c)).isEqualTo(beforeHead)
            assertThat(n(c, "SELECT next_file_id FROM meta")).isEqualTo(beforeFileId)
            assertThat(n(c, "SELECT next_row_id FROM tables WHERE dropped_snapshot IS NULL")).isZero()
            assertThat(n(c, "SELECT COUNT(*) FROM files")).isZero()
            assertThat(n(c, "SELECT COUNT(*) FROM receipts")).isZero()
            assertThat(
                q(c, "SELECT state FROM uploads WHERE path = ?", claim.path).firstRow()!![0],
            ).isEqualTo(if (mode == "abandon") "abandoned" else "active")
        }
    }

    @Test
    fun `ambiguous targets execute removal protection before refusing without allocations`() {
        for (kind in listOf("none", "data", "delete")) {
            val (c, table) = setup()
            val owner = UUID.randomUUID()
            val claim = store.uploads.claim(c, UUID.randomUUID(), owner, "s3://hoglake-it/$c/up", "data")
            val file = FileRegistration(claim.path, 10, 100, 50)
            val dvPath = "s3://hoglake-it/$c/ambiguous.dv"
            val beforeHead = head(c)
            val beforeFileId = n(c, "SELECT next_file_id FROM meta")
            val hook =
                Hook({ m, p, b -> isTx(m, p, b) && "INSERT INTO files" in b && claim.path in b }, before = {
                    if (kind != "none") {
                        val path = if (kind == "data") claim.path else dvPath
                        store.catalogOperation(c) {
                            store.runTx(
                                c,
                                HogliteCatalogStore.freshTxid("queued"),
                                null,
                                HogliteTx().apply {
                                    add(
                                        "INSERT INTO removals(path,file_kind,reason,queued_snapshot,queued_at," +
                                            "claimed_at,claimed_by) VALUES (?,?,'snapshot_expiry',0," +
                                            "'2026-11-03','2026-11-03','worker')",
                                        path,
                                        kind,
                                    )
                                },
                            )
                        }
                    }
                })
            hooks += hook
            seen.clear()
            val error =
                runCatching {
                    pstore.commit(
                        c,
                        CommitRequest(
                            idempotencyKey = owner,
                            readSnapshot = beforeHead,
                            allowPendingDeletes = true,
                            requireUnchangedTables = true,
                            appends = listOf(TableAppend("ns", "t", listOf(file, file), table.tableUuid)),
                            deletes =
                                listOf(
                                    TableDeletes(
                                        "ns",
                                        "t",
                                        listOf(
                                            DeleteFileRegistration(
                                                0,
                                                dvPath,
                                                1,
                                                10,
                                                dataFilePath = claim.path,
                                            ),
                                        ),
                                        table.tableUuid,
                                    ),
                                ),
                        ),
                    )
                }.exceptionOrNull()
            assertThat(hook.remaining).isZero()
            assertThat(seen.count { isWriteTxLine(it, claim.path) }).isEqualTo(1)
            val attempt = seen.single { isWriteTxLine(it, claim.path) }
            val removalGuard = attempt.indexOf("FROM removals")
            val targetGuard = attempt.indexOf("SELECT 1")
            val allocation = attempt.indexOf("UPDATE meta SET next_snapshot")
            assertThat(removalGuard).isGreaterThanOrEqualTo(0)
            assertThat(targetGuard).isGreaterThan(removalGuard)
            assertThat(allocation).isGreaterThan(targetGuard)
            if (kind == "none") {
                assertThat(error).isExactlyInstanceOf(HoglakeException.Validation::class.java)
                    .hasMessage("pending delete must target exactly one same-table append in this commit")
            } else {
                assertThat(error).isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
                    .hasMessage("a file path in this commit is queued for removal; claim a fresh path and retry")
            }
            assertThat(head(c)).isEqualTo(beforeHead)
            assertThat(n(c, "SELECT next_file_id FROM meta")).isEqualTo(beforeFileId)
            assertThat(n(c, "SELECT next_row_id FROM tables WHERE dropped_snapshot IS NULL")).isZero()
            assertThat(n(c, "SELECT COUNT(*) FROM files")).isZero()
            assertThat(n(c, "SELECT COUNT(*) FROM dvs")).isZero()
            assertThat(n(c, "SELECT COUNT(*) FROM receipts")).isZero()
            assertThat(q(c, "SELECT state FROM uploads WHERE path = ?", claim.path).firstRow()!![0]).isEqualTo("active")
        }
    }

    @Test
    fun `duplicate staged targets refuse before a publication transaction`() {
        val (c, table) = setup()
        val path = "s3://hoglake-it/$c/duplicate-target"
        val file = FileRegistration(path, 10, 100, 50)
        val dv = DeleteFileRegistration(0, "$path.dv", 1, 10, dataFilePath = path)
        val before = head(c)
        seen.clear()
        assertThatThrownBy {
            pstore.commit(
                c,
                CommitRequest(
                    idempotencyKey = UUID.randomUUID(),
                    readSnapshot = before,
                    allowPendingDeletes = true,
                    requireUnchangedTables = true,
                    appends = listOf(TableAppend("ns", "t", listOf(file), table.tableUuid)),
                    deletes =
                        listOf(
                            TableDeletes("ns", "t", listOf(dv, dv.copy(path = "$path.second.dv")), table.tableUuid),
                        ),
                ),
            )
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("the staged file $path has two deletion vectors in one commit")
        assertThat(seen.count { isWriteTxLine(it, path) }).isZero()
        assertThat(head(c)).isEqualTo(before)
        assertThat(n(c, "SELECT next_file_id FROM meta")).isEqualTo(1)
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM receipts")).isZero()
    }

    @Test
    fun `merged equal UUID expectations keep the transactional incarnation guard`() {
        val (c, table) = setup()
        val req = appendReq(c, "merged-uuid.parquet", expected = table.tableUuid)
        val hook =
            Hook({ m, p, b -> isTx(m, p, b) && "merged-uuid.parquet" in b }, before = {
                store.dropTable(c, "ns", "t", table.tableUuid, "test", "drop")
                store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            })
        hooks += hook
        val error =
            runCatching {
                pstore.commit(
                    c,
                    req.copy(
                        appends =
                            req.appends +
                                req.appends.map {
                                    val file = it.files.single()
                                    it.copy(files = listOf(file.copy(path = file.path + ".second")))
                                },
                    ),
                )
            }.exceptionOrNull()
        assertThat(hook.remaining).isZero()
        assertThat(error).isExactlyInstanceOf(HoglakeException.TableRecreated::class.java)
        assertThat((error as HoglakeException.TableRecreated).expectedTableUuid).isEqualTo(table.tableUuid)
        assertThat(error.table).isEqualTo("ns.t")
        assertThat(store.listFiles(c, "ns", "t")).isEmpty()
        assertThat(n(c, "SELECT COUNT(*) FROM receipts")).isZero()
        assertThat(n(c, "SELECT next_file_id FROM meta")).isEqualTo(1)
    }

    @Test
    fun `an ALTER between plan and tx replans once, twice is a typed 409`() {
        val (c, _) = setup()
        var k = 0

        fun alter() {
            store.alters.alterTable(
                c,
                "ns",
                "t",
                listOf(AlterOp.AddColumn(ColumnDef("x${k++}", ColType.LONG))),
                null,
                null,
                "p",
                "p",
            )
        }
        seen.clear()
        hooks += Hook({ m, p, b -> isTx(m, p, b) && "once.parquet" in b }, before = { alter() })
        val once = runCatching { pstore.commit(c, appendReq(c, "once.parquet", key = null)) }
        val onceAttempts = seen.count { isWriteTxLine(it, "once.parquet") }
        assertThat(once.isSuccess).isTrue()
        assertThat(onceAttempts).isEqualTo(2) // fence tripped, replanned, applied

        seen.clear()
        hooks += Hook({ m, p, b -> isTx(m, p, b) && "twice.parquet" in b }, remaining = 2, before = { alter() })
        val twice = runCatching { pstore.commit(c, appendReq(c, "twice.parquet", key = UUID.randomUUID())) }
        assertThat(twice.exceptionOrNull()).isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(
            store.listFiles(c, "ns", "t").map { it.path.substringAfterLast('/') },
        ).containsExactly("once.parquet")
    }

    @Test
    fun `DDL on an unrelated table does not trip a blind append's fence`() {
        val (c, _) = setup()
        store.createTable(c, "ns", "u", listOf(ColumnDef("id", ColType.LONG)))
        // A schema change to table U lands between the append's plan and tx.
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "calm.parquet" in b }, before = {
                store.alters.alterTable(
                    c,
                    "ns",
                    "u",
                    listOf(AlterOp.AddColumn(ColumnDef("z", ColType.LONG))),
                    null,
                    null,
                    "p",
                    "p",
                )
            })
        seen.clear()
        val r = runCatching { pstore.commit(c, appendReq(c, "calm.parquet", key = null)) }
        assertThat(r.isSuccess).isTrue()
        // No replan: exactly ONE tx carried the append.
        assertThat(seen.count { isWriteTxLine(it, "calm.parquet") }).isEqualTo(1)
    }

    @Test
    fun `recreate mid-commit is TableRecreated, drop mid-commit is TableDropped, guarded or not`() {
        val (c, t1) = setup()
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "rec.parquet" in b }, before = {
                store.dropTable(c, "ns", "t", t1.tableUuid, "p", "p")
                store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            })
        val rec = runCatching { pstore.commit(c, appendReq(c, "rec.parquet", expected = t1.tableUuid)) }
        assertThat(rec.exceptionOrNull()).isInstanceOf(HoglakeException.TableRecreated::class.java)

        val t2 = store.getTable(c, "ns", "t")
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "drp.parquet" in b }, before = {
                store.dropTable(c, "ns", "t", t2.tableUuid, "p", "p")
            })
        // Guarded (expected uuid): the DROP must type as TableDropped, not TableRecreated.
        val drp = runCatching { pstore.commit(c, appendReq(c, "drp.parquet", expected = t2.tableUuid)) }
        assertThat(drp.exceptionOrNull()).isInstanceOf(HoglakeException.TableDropped::class.java)

        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val t3 = store.getTable(c, "ns", "t")
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "ung.parquet" in b }, before = {
                store.dropTable(c, "ns", "t", t3.tableUuid, "p", "p")
            })
        val ung = runCatching { pstore.commit(c, appendReq(c, "ung.parquet")) }
        assertThat(ung.exceptionOrNull()).isInstanceOf(HoglakeException.TableDropped::class.java)
    }

    @Test
    fun `a namespace created between the drop's pre-check and fence keeps the catalog`() {
        val c = freshCatalog()
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "dropping = 1" in b }, before = { store.createNamespace(c, "late") })
        val r = runCatching { pstore.dropCatalog(c, "p", "p") }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        // The fence did not land: writes still work.
        store.createNamespace(c, "after")
    }

    @Test
    fun `an upload claim abandoned between pre-flight and tx stops the publication`() {
        val (c, _) = setup()
        val owner = UUID.randomUUID()
        val claim = store.uploads.claim(c, UUID.randomUUID(), owner, "s3://hoglake-it/$c/up", "data")
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && claim.path in b && "INSERT INTO files" in b }, before = {
                store.uploads.abandon(c, owner, listOf(claim.path))
            })
        // Valid footer metadata lets the claim race, not file validation, decide.
        val r =
            runCatching {
                pstore.commit(
                    c,
                    CommitRequest(
                        idempotencyKey = owner,
                        appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(claim.path, 1, 100, 5)))),
                    ),
                )
            }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(n(c, "SELECT COUNT(*) FROM files WHERE path = ?", claim.path)).isEqualTo(0L)
    }

    @Test
    fun `an applied response loss retries the exact materialization for keyed and unkeyed requests`() {
        for (key in listOf(null, UUID.randomUUID())) {
            val (c, _) = setup()
            val req = appendReq(c, "applied-loss.parquet", key)
            val captured = mutableListOf<com.fasterxml.jackson.databind.JsonNode>()
            val before = head(c)
            seen.clear()
            hooks +=
                Hook(
                    { m, p, b -> isTx(m, p, b) && "applied-loss.parquet" in b }, remaining = 2,
                    response = { status, bytes ->
                        assertThat(status).isEqualTo(200)
                        captured.add(wireObjectMapper().readTree(bytes))
                        if (captured.size == 1) {
                            503 to "{\"code\":\"outcome_unknown\"}".toByteArray()
                        } else {
                            status to bytes
                        }
                    },
                )
            val result = pstore.commit(c, req)
            val bodies =
                seen.filter {
                    isWriteTxLine(it, "applied-loss.parquet")
                }.map { it.substringAfter(" ", "").substringAfter(" ") }
            assertThat(bodies).hasSize(2)
            assertThat(bodies[0]).isEqualTo(bodies[1])
            val materialized = wireObjectMapper().readTree(bodies[0])
            assertThat(materialized["txid"].asText()).startsWith("ch:$c:")
            assertThat(materialized["fp"]).isNotNull()
            assertThat(materialized["fp"].asText()).isEqualTo(digest(req))
            assertThat(captured).hasSize(2)
            assertThat(captured[1]["dup"].asBoolean()).isTrue()
            assertThat(captured[1]["results"]).isEqualTo(captured[0]["results"])
            assertThat(captured[0]["results"].size()).isEqualTo(materialized["statements"].size())
            assertThat(result.snapshotId).isEqualTo(before + 1)
            assertThat(head(c)).isEqualTo(result.snapshotId)
            assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
        }
    }

    @Test
    fun `replay repair recovers identical receipts before settled claim and DV symptoms`() {
        for (mode in listOf("append", "claim", "delete")) {
            val (c, _) = setup()
            val key = UUID.randomUUID()
            val req =
                when (mode) {
                    "claim" -> {
                        val claim = store.uploads.claim(c, UUID.randomUUID(), key, "s3://hoglake-it/$c/up", "data")
                        appendReq(
                            c,
                            "unused",
                            key,
                        ).copy(
                            appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(claim.path, 10, 100, 50)))),
                        )
                    }
                    "delete" -> {
                        store.commit(c, appendReq(c, "seed.parquet"))
                        val id = n(c, "SELECT id FROM files")
                        dvReq(c, id, head(c), "replay-delete.puffin", 2).copy(idempotencyKey = key)
                    }
                    else -> appendReq(c, "replay-append.parquet", key)
                }
            var winner: CommitResult? = null
            var guardIndex = 0
            seen.clear()
            hooks +=
                Hook({ m, p, b -> isTx(m, p, b) && key.toString() in b }, before = {
                    winner = store.commit(c, req)
                }, response = { status, bytes ->
                    guardIndex = wireObjectMapper().readTree(bytes)["guard_index"]?.asInt() ?: 0
                    status to bytes
                })
            val replay = runCatching { pstore.commit(c, req) }
            assertThat(replay.exceptionOrNull()).isNull()
            assertThat(replay.getOrThrow()).isEqualTo(winner)
            assertThat(guardIndex).isEqualTo(2)
            assertThat(
                seen.count {
                    it.contains("/query ") && "SELECT fingerprint, snapshot_id, schema_version FROM receipts" in it
                },
            ).isEqualTo(1)
            assertThat(head(c)).isEqualTo(winner!!.snapshotId)
            assertThat(n(c, "SELECT COUNT(*) FROM receipts WHERE idempotency_key = ?", key.toString())).isEqualTo(1)
        }
    }

    @Test
    fun `replay repair uses a receipt across different upload settlement statement layouts`() {
        for (winnerHasClaim in listOf(true, false)) {
            val (c, _) = setup()
            val key = UUID.randomUUID()
            val claim = store.uploads.claim(c, UUID.randomUUID(), key, "s3://hoglake-it/$c/up", "data")
            val req =
                appendReq(
                    c,
                    "unused",
                    key,
                ).copy(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(claim.path, 10, 100, 50)))))
            // Keep the real claim definition. Only its visibility at each plan differs.
            if (winnerHasClaim) edit(c, "UPDATE uploads SET path = path || '.hidden'")
            var winner: CommitResult? = null
            seen.clear()
            hooks +=
                Hook({ m, p, b -> isTx(m, p, b) && claim.path in b }, before = {
                    if (winnerHasClaim) {
                        edit(c, "UPDATE uploads SET path = ?", claim.path)
                    } else {
                        edit(c, "DELETE FROM uploads")
                    }
                    winner = store.commit(c, req)
                })
            val replay = runCatching { pstore.commit(c, req) }
            assertThat(replay.exceptionOrNull()).isNull()
            assertThat(replay.getOrThrow()).isEqualTo(winner)
            val loser = seen.single { isWriteTxLine(it, claim.path) }
            assertThat("UPDATE uploads SET state = 'registered'" in loser).isEqualTo(!winnerHasClaim)
            assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
            assertThat(head(c)).isEqualTo(winner!!.snapshotId)
        }
    }

    @Test
    fun `replay repair shares the single replan budget with schema changes and receipt cleanup`() {
        for (schedule in listOf("purge", "purge-purge", "schema-purge", "purge-schema", "purge-retained")) {
            val (c, _) = setup()
            val key = UUID.randomUUID()
            val req = appendReq(c, "budget.parquet", key)
            var attempts = 0
            var lookups = 0
            var winner: CommitResult? = null
            seen.clear()
            hooks +=
                Hook({ m, p, b -> isTx(m, p, b) && "budget.parquet" in b }, remaining = 3, before = {
                    attempts++
                    val cause =
                        when {
                            schedule == "schema-purge" && attempts == 1 -> "schema"
                            schedule == "purge-schema" && attempts == 2 -> "schema"
                            schedule == "purge" && attempts == 2 -> "none"
                            else -> "receipt"
                        }
                    if (cause == "schema") {
                        HogliteAlters(store).alterTable(
                            c, "ns", "t",
                            listOf(AlterOp.AddColumn(ColumnDef("extra$attempts", ColType.LONG))),
                        )
                    }
                    if (cause == "receipt") winner = store.commit(c, req)
                })
            hooks +=
                Hook({ m, p, b -> receiptRead(m, p, b, key) }, remaining = 3, before = {
                    lookups++
                    if (!(schedule == "purge-retained" && attempts == 2)) {
                        edit(c, "DELETE FROM receipts WHERE idempotency_key = ?", key.toString())
                    }
                })
            val result = runCatching { pstore.commit(c, req) }
            assertThat(attempts).isEqualTo(2)
            if (schedule in listOf("purge", "purge-retained")) {
                assertThat(result.exceptionOrNull()).isNull()
                if (schedule == "purge-retained") assertThat(result.getOrThrow()).isEqualTo(winner)
                assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(2)
            } else {
                assertThat(result.exceptionOrNull()).isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
                assertThat(result.exceptionOrNull()!!.message).contains("retry")
            }
            assertThat(lookups).isEqualTo(if (schedule.startsWith("purge-p") || schedule == "purge-retained") 2 else 1)
            hooks.removeIf { it.remaining > 0 }
        }
    }

    @Test
    fun `replay repair compares all valid digest versions on collision and rejects malformed storage`() {
        SimpleMeterRegistry().let { registry ->
            Metrics.bind(registry)
            try {
                for (stored in listOf(
                    "00" + "ab".repeat(32),
                    "02" + "cd".repeat(32),
                    "ff" + "ef".repeat(32),
                    "01" + "00".repeat(32),
                    "02",
                    "zz".repeat(33),
                    "",
                )) {
                    for (different in listOf(false, true)) {
                        val (c, _) = setup()
                        val key = UUID.randomUUID()
                        val req = appendReq(c, "version-loser.parquet", key)
                        val winnerReq = if (different) appendReq(c, "version-winner.parquet", key) else req
                        var winner: CommitResult? = null
                        seen.clear()
                        hooks +=
                            Hook({ m, p, b -> isTx(m, p, b) && "version-loser.parquet" in b }, before = {
                                winner = store.commit(c, winnerReq)
                                edit(
                                    c, "UPDATE receipts SET fingerprint = ? WHERE idempotency_key = ?",
                                    stored, key.toString(),
                                )
                            })
                        val result = runCatching { pstore.commit(c, req) }
                        val unknown = stored.length == 66 && stored.take(2) in listOf("00", "02", "ff")
                        if (unknown) {
                            assertThat(result.exceptionOrNull()).isNull()
                            assertThat(result.getOrThrow()).isEqualTo(winner)
                        } else {
                            assertThat(
                                result.exceptionOrNull(),
                            ).isExactlyInstanceOf(HoglakeException.Validation::class.java)
                        }
                        val metric =
                            registry.find("hoglake_commit_receipt_unknown_digest_version_total")
                                .tag("catalog", c).counter()?.count() ?: 0.0
                        assertThat(metric).isEqualTo(if (unknown) 1.0 else 0.0)
                        assertThat(head(c)).isEqualTo(winner!!.snapshotId)
                        assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
                    }
                }
            } finally {
                Metrics.clear()
                registry.close()
            }
        }
    }

    @Test
    fun `replay repair malformed successful readback remains unknown without rematerialization`() {
        val mapper = wireObjectMapper()
        for (bad in listOf(
            "[]",
            "[[1]]",
            "[[1,2,3]]",
            "[[1,2],[3,4]]",
            "[[null,2]]",
            "[[\"1\",2]]",
            "[[1.5,2]]",
            "[[9223372036854775808,2]]",
        )) {
            val (c, _) = setup()
            val req = appendReq(c, "bad-readback.parquet")
            seen.clear()
            hooks +=
                Hook({ m, p, b -> isTx(m, p, b) && "bad-readback.parquet" in b }, response = { status, bytes ->
                    assertThat(status).isEqualTo(200)
                    val decoded = mapper.readTree(bytes)
                    val readback = decoded["results"].last() as com.fasterxml.jackson.databind.node.ObjectNode
                    readback.set<com.fasterxml.jackson.databind.JsonNode>("rows", mapper.readTree(bad))
                    status to mapper.writeValueAsBytes(decoded)
                })
            val result = runCatching { pstore.commit(c, req) }
            assertThat(result.exceptionOrNull()).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            val body = mapper.readTree(seen.single { isWriteTxLine(it, "bad-readback.parquet") }.substringAfter("/tx "))
            assertThat(
                (result.exceptionOrNull() as HoglakeException.CommitOutcomeUnknown).txid,
            ).isEqualTo(body["txid"].asText())
            assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
            assertThat(store.commit(c, req).snapshotId).isEqualTo(head(c))
        }
    }

    @Test
    fun `sticky ambiguity blocks both private markers after applied response loss`() {
        for (loss in listOf("504", "transport")) {
            for (marker in listOf("receipt", "schema")) {
                val (c, _) = setup()
                val key = UUID.randomUUID()
                val req = appendReq(c, "sticky-private.parquet", key)
                var responses = 0
                var guardIndex = 0
                var durable: com.fasterxml.jackson.databind.JsonNode? = null
                seen.clear()
                val hook =
                    Hook(
                        { m, p, b -> isTx(m, p, b) && "sticky-private.parquet" in b },
                        remaining = 2,
                        response = { status, bytes ->
                            responses++
                            assertThat(status).isEqualTo(200)
                            if (responses == 1) {
                                durable = wireObjectMapper().readTree(bytes)
                                val wire =
                                    wireObjectMapper().readTree(
                                        seen.last {
                                            isWriteTxLine(
                                                it,
                                                "sticky-private.parquet",
                                            )
                                        }.substringAfter("/tx "),
                                    )
                                val sql =
                                    if (marker == "receipt") {
                                        "SELECT COUNT(*) FROM receipts WHERE idempotency_key = ?"
                                    } else {
                                        "SELECT schema_version FROM tables WHERE id = ?"
                                    }
                                val statements = wire["statements"]
                                guardIndex = statements.indexOfFirst { statement ->
                                    statement["sql"].asText() == sql
                                } + 1
                                // A missing receipt makes an incorrect replan publish again.
                                edit(c, "DELETE FROM receipts WHERE idempotency_key = ?", key.toString())
                                if (loss == "504") {
                                    504 to "{\"code\":\"outcome_unknown\"}".toByteArray()
                                } else {
                                    status to bytes
                                }
                            } else {
                                409 to "{\"error\":\"expect_failed\",\"guard_index\":$guardIndex}".toByteArray()
                            }
                        },
                        dropResponse = { loss == "transport" && responses == 1 },
                    )
                hooks += hook
                val outcome = runCatching { pstore.commit(c, req) }
                assertThat(
                    outcome.exceptionOrNull(),
                ).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                assertThat(hook.remaining).describedAs("$loss/$marker: ${outcome.exceptionOrNull()?.message}").isZero()
                assertThat(guardIndex).isGreaterThan(0)
                val bodies =
                    seen.filter { isWriteTxLine(it, "sticky-private.parquet") }
                        .map { it.substringAfter("/tx ") }
                assertThat(bodies).hasSize(2)
                assertThat(bodies.distinct()).hasSize(1)
                val wire = wireObjectMapper().readTree(bodies.first())
                assertThat((outcome.exceptionOrNull() as HoglakeException.CommitOutcomeUnknown).txid)
                    .isEqualTo(wire["txid"].asText())
                assertThat(wire["fp"].asText()).isEqualTo(digest(req))
                assertThat(seen.count { isReadBatch(it) && key.toString() in it && "FROM receipts" in it }).isEqualTo(1)
                assertThat(seen.none { it.contains("/query ") && "SELECT fingerprint, snapshot_id" in it }).isTrue()
                assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
                val readback = durable!!["results"].last()["rows"].first()[0].asLong()
                assertThat(head(c)).isEqualTo(readback)
                assertThat(n(c, "SELECT COUNT(*) FROM receipts")).isZero()
            }
        }
    }

    @Test
    fun `not applied retries retain receipt recovery and the bounded schema replan`() {
        for (marker in listOf("receipt", "schema")) {
            val (c, _) = setup()
            val key = UUID.randomUUID()
            val req = appendReq(c, "known-private.parquet", key)
            var winner: CommitResult? = null
            seen.clear()
            hooks +=
                Hook(
                    { m, p, b -> isTx(m, p, b) && "known-private.parquet" in b },
                    shortCircuit = 503, shortCircuitBody = "{\"code\":\"not_applied\"}",
                )
            hooks +=
                Hook({ m, p, b -> isTx(m, p, b) && "known-private.parquet" in b }, before = {
                    if (marker == "receipt") {
                        winner = store.commit(c, req)
                    } else {
                        HogliteAlters(store).alterTable(
                            c, "ns", "t",
                            listOf(AlterOp.AddColumn(ColumnDef("new", ColType.LONG))),
                        )
                    }
                })
            val outcome = runCatching { pstore.commit(c, req) }
            assertThat(outcome.exceptionOrNull()).isNull()
            val bodies = seen.filter { isWriteTxLine(it, "known-private.parquet") }.map { it.substringAfter("/tx ") }
            assertThat(bodies.take(2).distinct()).hasSize(1)
            assertThat(bodies.map { wireObjectMapper().readTree(it)["fp"].asText() }.distinct())
                .containsExactly(digest(req))
            if (marker == "receipt") {
                assertThat(outcome.getOrThrow()).isEqualTo(winner)
                assertThat(bodies).hasSize(2)
                assertThat(
                    seen.count { it.contains("/query ") && "SELECT fingerprint, snapshot_id" in it },
                ).isEqualTo(1)
            } else {
                assertThat(bodies).hasSize(3)
                assertThat(bodies.distinct()).hasSize(2)
            }
            assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
        }
    }

    @Test
    fun `replay repair refuses unrelated failures without receipt recovery or a new materialization`() {
        val cases =
            listOf(
                Triple(
                    422,
                    "{\"code\":\"refused\",\"error\":\"receipts unrelated SQL failure\"}",
                    HoglakeException.Validation::class.java,
                ),
                Triple(
                    422,
                    "{\"error\":\"receipts ambiguous failure\"}",
                    HoglakeException.CommitOutcomeUnknown::class.java,
                ),
                Triple(503, "{\"code\":\"not_applied\"}", HoglakeException.BackendUnavailable::class.java),
                Triple(503, "{\"code\":\"outcome_unknown\"}", HoglakeException.CommitOutcomeUnknown::class.java),
                Triple(504, "{\"code\":\"outcome_unknown\"}", HoglakeException.CommitOutcomeUnknown::class.java),
                Triple(
                    409,
                    "{\"error\":\"expect_failed\",\"guard_index\":999}",
                    HoglakeException.CommitConflict::class.java,
                ),
            )
        for ((status, body, type) in cases) {
            val (c, _) = setup()
            val key = UUID.randomUUID()
            val req = appendReq(c, "unrelated-failure.parquet", key)
            val local =
                checkedStore(
                    HogliteClient(
                        standardUrl = "http://127.0.0.1:${proxy.address.port}",
                        retryBudgetMs = 300,
                        sleeper = {},
                    ),
                )
            seen.clear()
            val hook =
                Hook(
                    { m, p, b -> isTx(m, p, b) && "unrelated-failure.parquet" in b },
                    remaining = Int.MAX_VALUE,
                    shortCircuit = status,
                    shortCircuitBody = body,
                )
            hooks += hook
            try {
                val result = runCatching { local.commit(c, req) }
                assertThat(result.exceptionOrNull()).isExactlyInstanceOf(type)
                assertThat(
                    seen.none {
                        it.contains("/query ") && "SELECT fingerprint, snapshot_id, schema_version FROM receipts" in it
                    },
                ).isTrue()
                val attempts = seen.filter { isWriteTxLine(it, "unrelated-failure.parquet") }
                assertThat(attempts).isNotEmpty()
                assertThat(attempts.distinct()).hasSize(1)
                if (status == 422 || status == 409) assertThat(attempts).hasSize(1)
                if (result.exceptionOrNull() is HoglakeException.CommitOutcomeUnknown) {
                    val txid = wireObjectMapper().readTree(attempts.first().substringAfter("/tx "))["txid"].asText()
                    assertThat((result.exceptionOrNull() as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo(txid)
                }
                assertThat(n(c, "SELECT COUNT(*) FROM files")).isZero()
            } finally {
                hooks.remove(hook)
            }
        }
    }

    @Test
    fun `replay repair recovery read failures preserve known rollback typing`() {
        for (status in listOf(503, 404)) {
            val (c, _) = setup()
            val key = UUID.randomUUID()
            val req = appendReq(c, "failed-recovery.parquet", key)
            val local =
                checkedStore(
                    HogliteClient(
                        standardUrl = "http://127.0.0.1:${proxy.address.port}",
                        retryBudgetMs = 300,
                        sleeper = {},
                    ),
                )
            hooks +=
                Hook(
                    { m, p, b -> isTx(m, p, b) && "failed-recovery.parquet" in b },
                    before = { store.commit(c, req) },
                )
            val recovery =
                Hook(
                    { m, p, b -> receiptRead(m, p, b, key) },
                    remaining = Int.MAX_VALUE,
                    shortCircuit = status,
                    shortCircuitBody = "{\"code\":\"not_applied\"}",
                )
            hooks += recovery
            seen.clear()
            try {
                val result = runCatching { local.commit(c, req) }
                assertThat(
                    result.exceptionOrNull(),
                ).isExactlyInstanceOf(
                    if (status == 404) {
                        HoglakeException.NotFound::class.java
                    } else {
                        HoglakeException.BackendUnavailable::class.java
                    },
                )
                assertThat(recovery.remaining).isLessThan(Int.MAX_VALUE)
                assertThat(seen.count { isWriteTxLine(it, "failed-recovery.parquet") }).isEqualTo(1)
                assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
            } finally {
                hooks.remove(recovery)
            }
        }
    }

    @Test
    fun `replay repair replans upload ownership after receipt cleanup`() {
        val (c, _) = setup()
        val key = UUID.randomUUID()
        val other = UUID.randomUUID()
        val claim = store.uploads.claim(c, UUID.randomUUID(), other, "s3://hoglake-it/$c/up", "data")
        edit(c, "UPDATE uploads SET path = path || '.hidden'")
        val req =
            appendReq(
                c,
                "unused",
                key,
            ).copy(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(claim.path, 10, 100, 50)))))
        hooks += Hook({ m, p, b -> isTx(m, p, b) && claim.path in b }, before = { store.commit(c, req) })
        val recovery =
            Hook({ m, p, b -> receiptRead(m, p, b, key) }, before = {
                edit(c, "DELETE FROM receipts WHERE idempotency_key = ?", key.toString())
                edit(c, "UPDATE uploads SET path = ?", claim.path)
            })
        hooks += recovery
        seen.clear()
        val result = runCatching { pstore.commit(c, req) }
        assertThat(recovery.remaining).isZero()
        assertThat(result.exceptionOrNull()).isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(result.exceptionOrNull()!!.message).contains("upload")
        assertThat(seen.count { isReadBatch(it) && key.toString() in it && "FROM receipts" in it }).isEqualTo(2)
        assertThat(seen.count { isWriteTxLine(it, claim.path) }).isEqualTo(1)
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
        assertThat(q(c, "SELECT state FROM uploads").firstRow()!![0]).isEqualTo("active")
    }

    @Test
    fun `replay repair malformed or unequal preflight receipts refuse without a write`() {
        for (stored in listOf("", "02", "gg".repeat(33), "01" + "00".repeat(32))) {
            val (c, _) = setup()
            val req = appendReq(c, "bad-receipt.parquet")
            val first = store.commit(c, req)
            edit(c, "UPDATE receipts SET fingerprint = ?", stored)
            seen.clear()
            val result = runCatching { pstore.commit(c, req) }
            assertThat(result.exceptionOrNull()).isExactlyInstanceOf(HoglakeException.Validation::class.java)
            assertThat(seen.none { isWriteTxLine(it, "bad-receipt.parquet") }).isTrue()
            assertThat(head(c)).isEqualTo(first.snapshotId)
        }
        val (c, _) = setup()
        val req = appendReq(c, "uppercase.parquet")
        val first = store.commit(c, req)
        edit(c, "UPDATE receipts SET fingerprint = upper(fingerprint)")
        val replay = runCatching { pstore.commit(c, req) }
        assertThat(replay.exceptionOrNull()).isNull()
        assertThat(replay.getOrThrow()).isEqualTo(first)
    }

    @Test
    fun `replay repair ignores legacy permanent engine identities when the application receipt is absent`() {
        val (c, _) = setup()
        val key = UUID.randomUUID()
        val req = appendReq(c, "legacy-identity.parquet", key)
        boundTx(
            c,
            "ik:$c:$key",
            digest(req),
            HogliteTx().apply {
                add("UPDATE meta SET next_snapshot = next_snapshot WHERE id = 1")
            },
        )
        seen.clear()
        val result = runCatching { pstore.commit(c, req) }
        assertThat(result.exceptionOrNull()).isNull()
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
        assertThat(store.receipt(c, key)).isEqualTo(result.getOrThrow())
    }

    @Test
    fun `an applied response loss exhausts as unknown with one publication and resolves through its receipt`() {
        val (c, _) = setup()
        val req = appendReq(c, "all-responses-lost.parquet")
        val local =
            checkedStore(
                HogliteClient(
                    standardUrl = "http://127.0.0.1:${proxy.address.port}",
                    retryBudgetMs = 600,
                    sleeper = {},
                ),
            )
        val captured = mutableListOf<com.fasterxml.jackson.databind.JsonNode>()
        val hook =
            Hook(
                { m, p, b -> isTx(m, p, b) && "all-responses-lost.parquet" in b },
                remaining = Int.MAX_VALUE,
                response = { status, bytes ->
                    assertThat(status).isEqualTo(200)
                    captured.add(wireObjectMapper().readTree(bytes))
                    503 to "{\"code\":\"outcome_unknown\"}".toByteArray()
                },
            )
        hooks += hook
        seen.clear()
        try {
            val result = runCatching { local.commit(c, req) }
            assertThat(result.exceptionOrNull()).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            val attempts = seen.filter { isWriteTxLine(it, "all-responses-lost.parquet") }
            assertThat(attempts.size).isGreaterThan(1)
            assertThat(attempts.distinct()).hasSize(1)
            val txid = wireObjectMapper().readTree(attempts.first().substringAfter("/tx "))["txid"].asText()
            assertThat((result.exceptionOrNull() as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo(txid)
            assertThat(
                captured.drop(1).all { it["dup"].asBoolean() && it["results"] == captured[0]["results"] },
            ).isTrue()
            assertThat(n(c, "SELECT COUNT(*) FROM files")).isEqualTo(1)
            assertThat(store.commit(c, req).snapshotId).isEqualTo(head(c))
        } finally {
            hooks.remove(hook)
        }
    }

    @Test
    fun `snapshot timestamps never go backwards`() {
        val (c, _) = setup()
        store.commit(c, appendReq(c, "ts0.parquet"))
        // A future-stamped predecessor (pod-clock skew): the next snapshot's
        // ts honours it — time never moves backwards relative to id order.
        boundTx(
            c,
            HogliteCatalogStore.freshTxid("ts"),
            null,
            HogliteTx().apply { add("UPDATE snapshots SET ts = '2031-01-01T00:00:00.000Z' WHERE id = ${head(c)}") },
        )
        val r = store.commit(c, appendReq(c, "ts1.parquet"))
        val ts = q(c, "SELECT ts FROM snapshots WHERE id = ?", r.snapshotId).firstRow()!![0].asString()
        assertThat(ts).isGreaterThanOrEqualTo("2031-01-01T00:00:00.000Z")
    }

    @Test
    fun `alter allows at most two materializations and neither rollback consumes ids`() {
        val (c, original) = setup()
        val before = head(c)
        var interference = 0
        val bodies = mutableListOf<String>()
        val responses = mutableListOf<String>()
        val hook =
            Hook(::isAlterTx, remaining = 3, before = {
                store.alters.alterTable(
                    c,
                    "ns",
                    "t",
                    listOf(AlterOp.AddColumn(ColumnDef("a${interference++}", ColType.LONG))),
                )
            }, response = { status, bytes ->
                assertThat(status).isEqualTo(409)
                responses += bytes.decodeToString()
                status to bytes
            })
        hooks += hook
        val start = seen.size
        assertThatThrownBy {
            pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))))
        }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
        hooks.remove(hook)
        bodies += seen.drop(start).filter { "table_altered" in it && "POST " in it && "/tx " in it }
        assertThat(bodies).hasSize(2)
        assertThat(
            bodies.map { wireObjectMapper().readTree(it.substringAfter("/tx "))["txid"].asText() }.distinct(),
        ).hasSize(2)
        assertThat(responses).hasSize(2)
        assertThat(hook.remaining).isEqualTo(1)
        assertThat(head(c)).isEqualTo(before + 2)
        val current = store.getTable(c, "ns", "t")
        assertThat(current.columns.map { it.def.name }).containsExactly("id", "a0", "a1")
        assertThat(current.columns.map { it.fieldId }).containsExactly(1, 2, 3)
        assertThat(n(c, "SELECT COUNT(*) FROM changes WHERE kind = 'table_altered'")).isEqualTo(2)
        assertThat(store.getTable(c, "ns", "t", snapshot = original.readSnapshotId).columns).hasSize(1)
    }

    @Test
    fun `alter replan refuses replacement table namespace and renamed targets`() {
        for (replacement in listOf("table", "namespace", "rename")) {
            val (c, original) = setup()
            var replacementUuid: UUID? = null
            var hookError: Throwable? = null
            val hook =
                Hook(::isAlterTx, before = {
                    store.alters.alterTable(c, "ns", "t", listOf(AlterOp.SetTableComment("winner")))
                }, after = {
                    try {
                        if (replacement == "rename") {
                            store.alters.alterTable(c, "ns", "t", listOf(AlterOp.RenameTable("old")))
                        } else {
                            store.dropTable(c, "ns", "t", author = "test", message = "replace")
                            if (replacement != "table") {
                                store.dropNamespace(
                                    c,
                                    "ns",
                                    author = "test",
                                    message = "replace",
                                )
                            }
                            if (replacement != "table") store.createNamespace(c, "ns")
                        }
                        replacementUuid =
                            store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG))).tableUuid
                    } catch (e: Throwable) {
                        hookError = e
                        throw e
                    }
                })
            hooks += hook
            val start = seen.size
            val error =
                runCatching {
                    pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))))
                }.exceptionOrNull()
            assertThat(error).describedAs("replacement must refuse, not retarget").isNotNull()
            assertThat(hookError).describedAs("replacement fixture $replacement").isNull()
            assertThat(error).isExactlyInstanceOf(HoglakeException.TableRecreated::class.java)
            assertThat((error as HoglakeException.TableRecreated).expectedTableUuid).isEqualTo(original.tableUuid)
            assertThat(hook.remaining).isZero()
            assertThat(seen.drop(start).count { "/tx " in it && "table_altered" in it }).isEqualTo(1)
            assertThat(store.getTable(c, "ns", "t").tableUuid).isEqualTo(replacementUuid)
            assertThat(store.getTable(c, "ns", "t").columns.map { it.def.name }).containsExactly("id")
        }
    }

    @Test
    fun `alter does not replan other guard indices or arbitrary statuses`() {
        // Index 0 is the prepended catalog fence; 1 is table identity; 2
        // is the only private schema guard. Allocator and unknown indices
        // never share that marker, regardless of misleading error prose.
        val answers =
            listOf(
                409 to """{"code":"expect_failed","guard_index":-1,"error":"schema changed"}""",
                409 to """{"code":"expect_failed","guard_index":0,"error":"schema changed"}""",
                409 to
                    """{"code":"expect_failed","guard_index":1,"error":"table schema changed while planning alter"}""",
                409 to
                    """{"code":"expect_failed","guard_index":2,"error":"table schema changed while planning alter"}""",
                409 to
                    """{"code":"expect_failed","guard_index":9,"error":"table schema changed while planning alter"}""",
                409 to
                    """{"code":"expect_failed","guard_index":999,"error":"schema changed"}""",
                409 to """{"error":"table schema changed while planning alter"}""",
                422 to """{"error":"table schema changed while planning alter"}""",
                503 to """{"error":"table schema changed while planning alter"}""",
            )
        for ((status, body) in answers) {
            val (c, _) = setup()
            val before = head(c)
            val hook = Hook(::isAlterTx, remaining = Int.MAX_VALUE, shortCircuit = status, shortCircuitBody = body)
            hooks += hook
            val start = seen.size
            try {
                assertThatThrownBy {
                    pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))))
                }.isInstanceOf(HoglakeException::class.java)
                val sent = seen.drop(start).filter { "/tx " in it && "table_altered" in it }
                assertThat(sent).isNotEmpty()
                assertThat(
                    sent.distinct(),
                ).describedAs("status $status: transport retries byte-identical, no new plan").hasSize(1)
                assertThat(head(c)).isEqualTo(before)
                assertThat(store.getTable(c, "ns", "t").columns).hasSize(1)
            } finally {
                hooks.remove(hook)
            }
        }
    }

    @Test
    fun `alter lost acknowledgement and later schema refusal stay unknown with one materialization`() {
        val (c, _) = setup()
        val before = head(c)
        val lose = Hook(::isAlterTx, dropResponse = { true })
        // First really commits. The next transport attempt sees a schema-
        // looking refusal; it cannot settle the earlier ambiguous apply.
        val refusal =
            Hook(
                ::isAlterTx,
                remaining = Int.MAX_VALUE,
                shortCircuit = 409,
                shortCircuitBody = """{"code":"expect_failed","guard_index":3,"error":"schema changed"}""",
            )
        hooks += lose
        hooks += refusal
        val start = seen.size
        try {
            assertThatThrownBy {
                pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))))
            }.isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            val sent = seen.drop(start).filter { "/tx " in it && "table_altered" in it }
            assertThat(sent.size).isGreaterThan(1)
            assertThat(sent.distinct()).hasSize(1)
            assertThat(head(c)).isEqualTo(before + 1)
            assertThat(store.getTable(c, "ns", "t").columns.map { it.def.name }).containsExactly("id", "b")
        } finally {
            hooks.remove(refusal)
        }
    }

    @Test
    fun `alter aggregate failure never replans and a later writer cannot change its returned snapshot`() {
        for (fail in listOf(false, true)) {
            val (c, _) = setup()
            val before = head(c)
            val aggregate =
                Hook(
                    { m, p, b -> m == "POST" && p.endsWith("/query") && "SUM(record_count)" in b },
                    remaining = if (fail) Int.MAX_VALUE else 1,
                    shortCircuit = if (fail) 503 else null,
                    before = {
                        if (!fail) {
                            store.alters.alterTable(
                                c,
                                "ns",
                                "t",
                                listOf(AlterOp.AddColumn(ColumnDef("later", ColType.LONG))),
                            )
                            store.commit(c, appendReq(c, "later.parquet"))
                        }
                    },
                )
            val stale =
                Hook(::isAlterTx, before = {
                    store.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("winner", ColType.LONG))))
                })
            hooks += stale
            hooks += aggregate
            val start = seen.size
            try {
                if (fail) {
                    assertThatThrownBy {
                        pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))))
                    }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                } else {
                    val result =
                        pstore.alters.alterTable(
                            c,
                            "ns",
                            "t",
                            listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))),
                        )
                    assertThat(result.readSnapshotId).isEqualTo(before + 2)
                    assertThat(result.columns.map { it.def.name }).containsExactly("id", "winner", "b")
                    assertThat(result.recordCount).isZero()
                    assertThat(result.fileCount).isZero()
                    assertThat(result.fileSizeBytes).isZero()
                    assertThat(store.getTable(c, "ns", "t").recordCount).isEqualTo(10)
                }
                assertThat(seen.drop(start).count { "/tx " in it && "table_altered" in it }).isEqualTo(2)
                assertThat(head(c)).isEqualTo(before + if (fail) 2 else 4)
            } finally {
                hooks.remove(stale)
                hooks.remove(aggregate)
            }
        }
    }

    @Test
    fun `alter stats conflict never consumes a schema retry`() {
        val c = freshCatalog()
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.INT)))
        val bound = com.posthog.hoglake.stats.IcebergSingleValue.encode(ColType.INT, 7)
        store.commit(
            c,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration(
                                    "s3://hoglake-it/$c/data/stats.parquet",
                                    10,
                                    100,
                                    50,
                                    listOf(ColumnStats(1, 10, 0, null, null, bound, bound)),
                                ),
                            ),
                        ),
                    ),
            ),
        )
        val before = head(c)
        val changed = listOf(ColumnStats(1, 10, 1, null, null, bound, bound))
        val hook =
            Hook(::isAlterTx, before = {
                store.catalogOperation(c) {
                    val tx = HogliteTx()
                    tx.add("UPDATE files SET stats_json = ?", store.json.writeValueAsString(changed))
                    store.runTx(c, HogliteCatalogStore.freshTxid("stats"), null, tx)
                }
            })
        hooks += hook
        val start = seen.size
        assertThatThrownBy {
            pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.PromoteColumn("id", ColType.LONG)))
        }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessage("file stats changed under the alter")
        assertThat(hook.remaining).isZero()
        assertThat(seen.drop(start).count { "/tx " in it && "table_altered" in it }).isEqualTo(1)
        assertThat(head(c)).isEqualTo(before)
        assertThat(store.getTable(c, "ns", "t").columns.single().def.type).isEqualTo(ColType.INT)
        assertThat(q(c, "SELECT stats_json FROM files").firstRow()!![0].asString())
            .isEqualTo(store.json.writeValueAsString(changed))
    }

    @Test
    fun `unrelated table DDL does not cause an alter replan`() {
        val (c, _) = setup()
        store.createTable(c, "ns", "other", listOf(ColumnDef("id", ColType.LONG)))
        val hook =
            Hook(::isAlterTx, before = {
                store.alters.alterTable(c, "ns", "other", listOf(AlterOp.AddColumn(ColumnDef("a", ColType.LONG))))
            })
        hooks += hook
        val start = seen.size
        val result = pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))))
        assertThat(result.columns.map { it.def.name }).containsExactly("id", "b")
        assertThat(hook.remaining).isZero()
        assertThat(seen.drop(start).count { "/tx " in it && "table_altered" in it }).isEqualTo(1)
    }

    @Test
    fun `guarded alter never replans even a schema marker without a changes row`() {
        val (c, original) = setup()
        // Isolate the schema fence (synthetic protocol-negative control):
        // normal production DDL also adds a changes row and hits the earlier
        // DdlSinceReadSnapshot guard, covered by the shared real race.
        val hook =
            Hook(::isAlterTx, before = {
                store.catalogOperation(c) {
                    val tx = HogliteTx()
                    tx.add("UPDATE tables SET schema_version = schema_version + 1")
                    store.runTx(c, HogliteCatalogStore.freshTxid("fence-control"), null, tx)
                }
            })
        hooks += hook
        val start = seen.size
        assertThatThrownBy {
            pstore.alters.alterTable(
                c,
                "ns",
                "t",
                listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))),
                readSnapshot = original.readSnapshotId,
            )
        }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(seen.drop(start).count { "/tx " in it && "table_altered" in it }).isEqualTo(1)
        assertThat(head(c)).isEqualTo(original.readSnapshotId)
    }

    @Test
    fun `invalid alter success after a real commit is unknown without another plan`() {
        val (c, _) = setup()
        val before = head(c)
        val hook =
            Hook(::isAlterTx, response = { status, _ ->
                assertThat(status).isEqualTo(200)
                200 to """{"results":[]}""".toByteArray()
            })
        hooks += hook
        val start = seen.size
        assertThatThrownBy {
            pstore.alters.alterTable(c, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("b", ColType.LONG))))
        }.isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(seen.drop(start).count { "/tx " in it && "table_altered" in it }).isEqualTo(1)
        assertThat(head(c)).isEqualTo(before + 1)
        assertThat(store.getTable(c, "ns", "t").columns.map { it.def.name }).containsExactly("id", "b")
    }

    @Test
    fun `a replace-if-absent publication refuses a table created after its preflight`() {
        val c = freshCatalog()
        store.createNamespace(c, "ns")
        val (op, _) = prepareCreation(c, "fresh", com.posthog.hoglake.service.ReplacementTarget(null, head(c)))
        // The preflight found the name free (as at read_snapshot); a table
        // takes it before the publication tx. Publishing would put a second
        // live table under the name or silently retire the newcomer.
        val hook =
            Hook(
                { m, p, b -> m == "POST" && p.endsWith("/query") && "SELECT t.id, t.table_uuid FROM tables t" in b },
                after = { store.createTable(c, "ns", "fresh", listOf(ColumnDef("id", ColType.LONG))) },
            )
        hooks += hook
        val newcomer = lazy { store.getTable(c, "ns", "fresh").tableUuid }
        val published = pstore.tableCreations.publish(c, op, emptyList())
        assertThat(hook.remaining).isZero()
        assertThat(published.state).isEqualTo("rejected")
        assertThat(published.reason).isEqualTo("target_changed")
        assertThat(store.listTables(c, "ns").map { it.tableUuid }).containsExactly(newcomer.value)
    }

    @Test
    fun `a view create re-asserts its namespace inside the transaction`() {
        val c = freshCatalog()
        store.createNamespace(c, "vns")
        // The create has resolved the namespace; the namespace is dropped
        // (no views yet, so the drop's check passes) before the insert.
        val hook =
            Hook(
                {
                        m,
                        p,
                        b,
                    ->
                    m == "POST" && p.endsWith("/query") && "FROM namespaces WHERE name = ?" in b
                },
                after = { store.dropNamespace(c, "vns", null, "test", "drop") },
            )
        hooks += hook
        assertThatThrownBy { pstore.views.create(c, "vns", "v", "SELECT 1", "trino", "test", "create") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM views")).isZero()
        // A recreated namespace takes the view, and every read reaches it.
        store.createNamespace(c, "vns")
        val view = store.views.create(c, "vns", "v", "SELECT 1", "trino", "test", "create")
        assertThat(store.views.list(c, "vns").map { it.viewUuid }).containsExactly(view.viewUuid)
        assertThat(store.views.get(c, "vns", "v").viewUuid).isEqualTo(view.viewUuid)
    }

    private fun partitionedWithFile(
        c: String,
        path: String?,
    ): com.posthog.hoglake.model.TableInfo {
        store.createNamespace(c, "ns")
        val t = store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        HogliteAlters(store).alterTable(
            c,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        if (path != null) {
            store.commit(
                c,
                CommitRequest(
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(
                                    FileRegistration("s3://hoglake-it/$c/data/$path", 10, 100, 50, null, listOf("7")),
                                ),
                            ),
                        ),
                ),
            )
        }
        return store.getTable(c, "ns", "t")
    }

    private class TenantTraffic(
        val queries: Int,
        val readBatches: Int,
        val writes: Int,
        val metaReads: Int,
    )

    private fun traffic(tenant: String): TenantTraffic {
        val lines = seen.filter { it.startsWith("POST /v1/t/$tenant/") }
        return TenantTraffic(
            queries = lines.count { it.startsWith("POST /v1/t/$tenant/query ") },
            readBatches = lines.count { it.startsWith("POST /v1/t/$tenant/tx ") && isReadBatch(it) },
            writes = lines.count { it.startsWith("POST /v1/t/$tenant/tx ") && !isReadBatch(it) },
            // Occurrences, not lines: a batch carrying the meta read twice is a duplicate read.
            metaReads = lines.sumOf { it.windowed(store.metaSql.length).count { w -> w == store.metaSql } },
        )
    }

    /**
     * Each counting scenario uses a separate catalog.
     * Scenarios cover blind appends, keyed appends, and guarded two-table commits.
     * The guarded commit includes a durable DV on a layout table.
     */
    private fun commitShapes(): List<Pair<String, (String) -> CommitRequest>> =
        listOf(
            "unkeyed blind append" to { c -> appendReq(c, "blind.parquet", key = null, table = "u") },
            "keyed append with read_snapshot" to { c -> appendReq(c, "keyed.parquet", read = head(c), table = "u") },
            "guarded append + durable DV on a layout table" to { c ->
                val fileId = n(c, "SELECT id FROM files WHERE path LIKE '%/seed.parquet'")
                CommitRequest(
                    readSnapshot = head(c),
                    idempotencyKey = UUID.randomUUID(),
                    requireUnchangedTables = true,
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "u",
                                listOf(FileRegistration("s3://hoglake-it/$c/data/u.parquet", 10, 100, 50)),
                            ),
                        ),
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        fileId,
                                        "s3://hoglake-it/$c/data/ns/t/id=7/dv.puffin",
                                        1,
                                        10,
                                    ),
                                ),
                            ),
                        ),
                )
            },
        )

    /** State for [commitShapes]: [layoutOn]'s ns.t, and ns.u plain. */
    private fun shapeCatalog(): String {
        val c = freshCatalog()
        layoutOn(c)
        store.createTable(c, "ns", "u", listOf(ColumnDef("id", ColType.LONG)))
        return c
    }

    /**
     * ns.t on [c], partitioned by identity(id) with the object-prefix layout
     * on it (set while empty, as the option requires) and one file for
     * id=7 under its prefix, stats deferred.
     */
    private fun layoutOn(c: String) {
        partitionedWithFile(c, null)
        HogliteAlters(store).alterTable(c, "ns", "t", listOf(AlterOp.SetObjectPrefixField("id")))
        store.commit(
            c,
            CommitRequest(
                readSnapshot = head(c),
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration(
                                    "s3://hoglake-it/$c/data/ns/t/id=7/seed.parquet",
                                    10,
                                    100,
                                    50,
                                    null,
                                    listOf("7"),
                                ),
                            ),
                        ),
                    ),
            ),
        )
    }

    // ---- planning reads see every definition their fence admits ----------

    /**
     * The liveTable read carries the table's schema fence.
     * A commit includes it and the meta read in one read-only `/tx`; so
     * does an alter (HogliteCatalogStore.headTableRow), whose statement
     * also selects the head beside the fence. Matched on the two
     * fragments every spelling of the read shares.
     */
    private fun isLiveTableRead(
        m: String,
        p: String,
        b: String,
    ) = m == "POST" &&
        (p.endsWith("/query") || (p.endsWith("/tx") && isReadBatch(b))) &&
        "t.next_row_id, t.schema_version" in b &&
        "FROM table_versions v" in b

    @Test
    fun `a commit plans with one read-only transaction, not one query per read`() {
        for ((shape, request) in commitShapes()) {
            val c = shapeCatalog()
            val tenant = store.catalogOperation(c) { store.boundTenant(c) }
            // Warm this process's per-tenant schema probe, so only the commit is counted.
            pstore.getCatalog(c)
            val req = request(c)
            seen.clear()
            pstore.commit(c, req)
            val t = traffic(tenant)
            assertThat(t.readBatches).describedAs("$shape: read-only transactions").isEqualTo(1)
            assertThat(t.queries).describedAs("$shape: single-read queries on the catalog").isZero()
            assertThat(t.writes).describedAs("$shape: write transactions").isEqualTo(1)
            // The request-scoped resolver adds no registry read.
            val reads = seen.count { it.startsWith("POST") && (it.contains("/query ") || isReadBatch(it)) }
            assertThat(reads).describedAs("$shape: reads per commit").isEqualTo(1)
        }
    }

    @Test
    fun `a commit reads the catalog meta row once per attempt`() {
        for ((shape, request) in commitShapes()) {
            val c = shapeCatalog()
            val tenant = store.catalogOperation(c) { store.boundTenant(c) }
            pstore.getCatalog(c)
            val req = request(c)
            seen.clear()
            pstore.commit(c, req)
            // The plan head is the batch's own meta.head: one snapshot is the
            // fence the old separate re-read approximated, so a second meta
            // read is a duplicate, never a fence.
            assertThat(traffic(tenant).metaReads).describedAs(shape).isEqualTo(1)
        }
        // A replan is a new attempt at a new snapshot: one meta read EACH.
        val (c, _) = setup()
        val tenant = store.catalogOperation(c) { store.boundTenant(c) }
        pstore.getCatalog(c)
        hooks +=
            Hook({ m, p, b -> isTx(m, p, b) && "replan-meta.parquet" in b }, before = {
                store.alters.alterTable(
                    c,
                    "ns",
                    "t",
                    listOf(AlterOp.AddColumn(ColumnDef("m", ColType.LONG))),
                    null,
                    null,
                    "p",
                    "p",
                )
            })
        seen.clear()
        pstore.commit(c, appendReq(c, "replan-meta.parquet", key = null))
        val t = traffic(tenant)
        assertThat(t.writes).isEqualTo(2)
        assertThat(t.readBatches).isEqualTo(2)
        assertThat(t.metaReads).isEqualTo(2)
    }

    @Test
    fun `a layout set between a commit's head read and its table read is enforced on the append`() {
        val c = freshCatalog()
        partitionedWithFile(c, null)
        // The layout lands just before the commit's planning read. When the
        // head and the table row were separate reads, it could land between
        // them: the fence the commit reads INCLUDES the layout, so a plan made
        // at the earlier head (no layout) would pass the in-tx guard with a
        // stray path. One read-only transaction reads both at one snapshot.
        val hook =
            Hook(::isLiveTableRead, before = {
                HogliteAlters(store).alterTable(c, "ns", "t", listOf(AlterOp.SetObjectPrefixField("id")))
            })
        hooks += hook
        val stray =
            CommitRequest(
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration(
                                    "s3://hoglake-it/$c/data/stray.parquet",
                                    10,
                                    100,
                                    50,
                                    null,
                                    listOf("7"),
                                ),
                            ),
                        ),
                    ),
            )
        assertThatThrownBy { pstore.commit(c, stray) }
            .isInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getTable(c, "ns", "t").objectPrefixField).isEqualTo("id")
        assertThat(n(c, "SELECT COUNT(*) FROM files")).isZero()
    }

    @Test
    fun `a spec replaced between an alter's head read and its table read is the spec it validates`() {
        val c = freshCatalog()
        partitionedWithFile(c, null)
        // set_object_prefix_field needs exactly one identity field on the
        // column. Unpartitioning lands after the alter read the old head but
        // before it read the fence: validating the old spec would set a
        // layout no current spec can honour.
        val hook =
            Hook(::isLiveTableRead, before = {
                HogliteAlters(store).alterTable(c, "ns", "t", listOf(AlterOp.SetPartitionSpec(emptyList())))
            })
        hooks += hook
        assertThatThrownBy {
            HogliteAlters(pstore).alterTable(c, "ns", "t", listOf(AlterOp.SetObjectPrefixField("id")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getTable(c, "ns", "t").objectPrefixField).isNull()
        assertThat(n(c, "SELECT COUNT(*) FROM table_layouts")).isZero()
    }

    @Test
    fun `a fence-poisoned scope stays poisoned even if the old tenant would accept the retry`() {
        val c = freshCatalog()
        val old = store.catalogOperation(c) { store.boundTenant(c) }
        var first: Throwable? = null
        var retry: Throwable? = null
        var sent = -1
        val r =
            runCatching {
                pstore.catalogOperation(c) {
                    pstore.meta(c)
                    client.tx(
                        old,
                        "poison-fence",
                        null,
                        listOf(HogliteStatement("UPDATE meta SET dropping = 1 WHERE id = 1", emptyList())),
                    )
                    first = runCatching { pstore.createNamespace(c, "a") }.exceptionOrNull()
                    client.tx(
                        old,
                        "poison-unfence",
                        null,
                        listOf(HogliteStatement("UPDATE meta SET dropping = 0 WHERE id = 1", emptyList())),
                    )
                    val before = seen.size
                    retry = runCatching { pstore.createNamespace(c, "a") }.exceptionOrNull()
                    sent = seen.size - before
                    "acknowledged"
                }
            }
        assertThat(first).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(retry).isSameAs(first)
        assertThat(sent).isZero()
        assertThat(r.exceptionOrNull()).isSameAs(first)
        assertThat(store.listNamespaces(c, null, null)).isEmpty()
        // and a new operation (old tenant now unfenced) proceeds
        assertThat(pstore.createNamespace(c, "a").name).isEqualTo("a")
    }
}
