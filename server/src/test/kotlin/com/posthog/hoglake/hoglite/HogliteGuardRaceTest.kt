package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.testing.TestImages
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The hoglite backend's in-transaction guards, exercised DETERMINISTICALLY:
 * an interposing HTTP proxy runs a concurrent write between a store call's
 * pre-flight reads and its transaction, which is the exact interleaving
 * every OCC guard exists for. Where the guards are right, the racing call
 * gets the typed conflict; where they are missing, the race is a silent
 * lost update — these tests are the pin.
 *
 * The proxy is also where the drop-lifecycle races live: a drop paused
 * mid-flight against a recreated catalog must never touch the new
 * incarnation.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteGuardRaceTest {
    private lateinit var minio: MinIOContainer
    private val procs = mutableListOf<Process>()
    private lateinit var client: HogliteClient
    private lateinit var store: HogliteCatalogStore
    private lateinit var pstore: HogliteCatalogStore
    private lateinit var proxy: HttpServer
    private var catalogCounter = 0
    private lateinit var binary: String
    private lateinit var s3Endpoint: String
    private var hogPort = 0

    /** A proxy hook: matched on (method, path, body); runs [before], then forwards (or short-circuits). */
    class Hook(
        val match: (String, String, String) -> Boolean,
        var remaining: Int = 1,
        val shortCircuit: Int? = null,
        val before: () -> Unit = {},
        val after: () -> Unit = {},
    )

    private val hooks = CopyOnWriteArrayList<Hook>()
    private val seen = ConcurrentLinkedQueue<String>()

    private fun startPod(
        id: String,
        port: Int,
        prefix: String,
        peers: String = "",
    ) {
        val dir = Files.createTempDirectory("hoglited-$id").toFile()
        val args =
            mutableListOf(
                binary, "-addr", "127.0.0.1:$port", "-dir", dir.absolutePath, "-bucket", "hoglite-it",
                "-id", id, "-class", "standard", "-prefix", prefix,
            )
        if (peers.isNotEmpty()) args += listOf("-peers", peers)
        val b = ProcessBuilder(args).redirectErrorStream(true)
        b.environment().putAll(
            mapOf(
                "AWS_ENDPOINT_URL" to s3Endpoint,
                "AWS_ACCESS_KEY_ID" to minio.userName,
                "AWS_SECRET_ACCESS_KEY" to minio.password,
                "AWS_REGION" to "us-east-1",
            ),
        )
        val p = b.start()
        procs += p
        val http = HttpClient.newHttpClient()
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (System.nanoTime() < deadline) {
            try {
                if (http.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/healthz")).build(),
                        HttpResponse.BodyHandlers.discarding(),
                    ).statusCode() == 200
                ) {
                    return
                }
            } catch (_: Exception) {
                // not up yet
            }
            Thread.sleep(100)
        }
        error("pod $id did not come up")
    }

    @BeforeAll
    fun start() {
        val env = System.getenv("HOGLITE_BINARY")
        assumeTrue(!env.isNullOrEmpty()) { "no hoglited binary (set HOGLITE_BINARY); skipping hoglite race tests" }
        binary = env
        minio = TestImages.minio()
        minio.start()
        // IP-literal endpoint: the AWS SDK only falls back to path-style
        // addressing for one; MinIOContainer's s3URL is a `localhost` URL.
        s3Endpoint = "http://127.0.0.1:${minio.firstMappedPort}"
        s3().use { s3 ->
            s3.createBucket { it.bucket("hoglite-it") }
            s3.createBucket { it.bucket("hoglake-it") }
        }
        hogPort = ServerSocket(0).use { it.localPort }
        startPod("race-1", hogPort, "race")
        client = HogliteClient(standardUrl = "http://127.0.0.1:$hogPort", sleeper = {})
        store = checkedStore(client)

        val fwd = HttpClient.newHttpClient()
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
                    respond(hook.shortCircuit, """{"error":"probe short-circuit"}""".toByteArray())
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
                val resp = fwd.send(req, HttpResponse.BodyHandlers.ofByteArray())
                hook?.after?.invoke()
                respond(resp.statusCode(), resp.body())
            } catch (e: Throwable) {
                ex.sendResponseHeaders(599, -1)
                ex.close()
            }
        }
        proxy.start()
        pstore =
            checkedStore(
                HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}", sleeper = {}),
            )
    }

    @AfterAll
    fun stop() {
        if (::proxy.isInitialized) proxy.stop(0)
        procs.forEach {
            it.destroy()
            it.waitFor()
        }
        if (::minio.isInitialized) minio.stop()
    }

    // ---- helpers ------------------------------------------------------

    private fun s3(): S3Client =
        S3Client.builder()
            .endpointOverride(URI.create(s3Endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.userName, minio.password)),
            )
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build()

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

    private fun isTx(
        m: String,
        p: String,
    ) = m == "POST" && p.endsWith("/tx")

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

    // ---- expiry: the sweep re-reads its policy inside the transaction ----

    @Test
    fun `a concurrent sweep cannot move the floor backwards`() {
        val (c, _) = setup()
        repeat(6) { store.commit(c, appendReq(c, "e$it.parquet")) }
        shortRetention(c)
        ageSnapshots(c)
        val pre = store.getCatalog(c).earliestSnapshotId
        // Between the slow sweep's plan and its advance tx, a fast sweep runs to completion.
        hooks +=
            Hook({ m, p, b -> isTx(m, p) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
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
            Hook({ m, p, b -> isTx(m, p) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
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
            Hook({ m, p, b -> isTx(m, p) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
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
            Hook({ m, p, b -> isTx(m, p) && "INSERT INTO offsets" in b }, before = {
                HogliteExpiry(store, purgePage = 100, purgeBudgetMs = 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
            })
        assertThatThrownBy { pstore.commitOffset(c, "consumer-1", t.tableUuid, 1) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        // The sweep really did advance the floor past the refused offset.
        assertThat(store.getCatalog(c).earliestSnapshotId).isGreaterThan(1L)
        assertThatThrownBy { store.getOffset(c, "consumer-1", t.tableUuid) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    // ---- commit: the per-table schema fence ----------------------------

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
        hooks += Hook({ m, p, b -> isTx(m, p) && "once.parquet" in b }, before = { alter() })
        val once = runCatching { pstore.commit(c, appendReq(c, "once.parquet", key = null)) }
        val onceAttempts = seen.count { it.startsWith("POST") && it.contains("/tx ") && "once.parquet" in it }
        assertThat(once.isSuccess).isTrue()
        assertThat(onceAttempts).isEqualTo(2) // fence tripped, replanned, applied

        seen.clear()
        hooks += Hook({ m, p, b -> isTx(m, p) && "twice.parquet" in b }, remaining = 2, before = { alter() })
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
            Hook({ m, p, b -> isTx(m, p) && "calm.parquet" in b }, before = {
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
        assertThat(seen.count { it.startsWith("POST") && it.contains("/tx ") && "calm.parquet" in it }).isEqualTo(1)
    }

    @Test
    fun `recreate mid-commit is TableRecreated, drop mid-commit is TableDropped, guarded or not`() {
        val (c, t1) = setup()
        hooks +=
            Hook({ m, p, b -> isTx(m, p) && "rec.parquet" in b }, before = {
                store.dropTable(c, "ns", "t", t1.tableUuid, "p", "p")
                store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            })
        val rec = runCatching { pstore.commit(c, appendReq(c, "rec.parquet", expected = t1.tableUuid)) }
        assertThat(rec.exceptionOrNull()).isInstanceOf(HoglakeException.TableRecreated::class.java)

        val t2 = store.getTable(c, "ns", "t")
        hooks +=
            Hook({ m, p, b -> isTx(m, p) && "drp.parquet" in b }, before = {
                store.dropTable(c, "ns", "t", t2.tableUuid, "p", "p")
            })
        // Guarded (expected uuid): the DROP must type as TableDropped, not TableRecreated.
        val drp = runCatching { pstore.commit(c, appendReq(c, "drp.parquet", expected = t2.tableUuid)) }
        assertThat(drp.exceptionOrNull()).isInstanceOf(HoglakeException.TableDropped::class.java)

        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val t3 = store.getTable(c, "ns", "t")
        hooks +=
            Hook({ m, p, b -> isTx(m, p) && "ung.parquet" in b }, before = {
                store.dropTable(c, "ns", "t", t3.tableUuid, "p", "p")
            })
        val ung = runCatching { pstore.commit(c, appendReq(c, "ung.parquet")) }
        assertThat(ung.exceptionOrNull()).isInstanceOf(HoglakeException.TableDropped::class.java)
    }

    // ---- drop lifecycle -------------------------------------------------

    @Test
    fun `a namespace created between the drop's pre-check and fence keeps the catalog`() {
        val c = freshCatalog()
        hooks += Hook({ m, p, b -> isTx(m, p) && "dropping = 1" in b }, before = { store.createNamespace(c, "late") })
        val r = runCatching { pstore.dropCatalog(c, "p", "p") }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        // The fence did not land: writes still work.
        store.createNamespace(c, "after")
    }

    @Test
    fun `a drop whose tenant DELETE fails leaves the catalog write-fenced until a retried drop`() {
        val c = freshCatalog()
        hooks += Hook({ m, p, _ -> m == "DELETE" && p.startsWith("/v1/tenants/") }, remaining = 3, shortCircuit = 409)
        assertThatThrownBy { pstore.dropCatalog(c, "p", "p") }
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        // Fenced: writes get the dropping 404; the catalog still lists.
        assertThatThrownBy { store.createNamespace(c, "x") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("being dropped")
        assertThat(store.listCatalogs().map { it.name }).contains(c)
        // A retried drop completes.
        store.dropCatalog(c, "p", "p")
        assertThat(store.listCatalogs().map { it.name }).doesNotContain(c)
    }

    @Test
    fun `a stale drop resuming after a recreate cannot touch the new incarnation`() {
        val c = freshCatalog()
        // Pause the stale drop AFTER its fence landed, BEFORE the release.
        val fenceLanded = CountDownLatch(1)
        val resume = CountDownLatch(1)
        hooks +=
            Hook({ m, p, b -> m == "POST" && p.endsWith("/release") && p.contains("/v1/t/hoglake-$c") }, before = {
                fenceLanded.countDown()
                resume.await(30, TimeUnit.SECONDS)
            })
        val staleDrop =
            Executors.newSingleThreadExecutor().submit<Any?> {
                try {
                    pstore.dropCatalog(c, "stale", "the paused drop")
                } catch (e: Throwable) {
                    e
                }
            }
        assertThat(fenceLanded.await(30, TimeUnit.SECONDS)).isTrue()
        // A second drop completes, the name is recreated, and a writer
        // commits to the NEW catalog.
        store.dropCatalog(c, "second", "the drop that finishes")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.commit(c, appendReq(c, "new.parquet"))
        resume.countDown()
        val outcome = staleDrop.get(30, TimeUnit.SECONDS)
        // The stale drop FINISHES (the incarnation it fenced is gone — its
        // release/delete are idempotent no-ops) and the new catalog is whole.
        assertThat(outcome !is Throwable).isTrue()
        assertThat(store.listFiles(c, "ns", "t")).hasSize(1)
        assertThat(store.listCatalogs().map { it.name }).contains(c)
    }

    @Test
    fun `multi-pod - a drop through a non-owner pod succeeds`() {
        val pc = ServerSocket(0).use { it.localPort }
        val pd = ServerSocket(0).use { it.localPort }
        val peers = "pod-c=http://127.0.0.1:$pc,pod-d=http://127.0.0.1:$pd"
        startPod("pod-c", pc, "mp", peers)
        startPod("pod-d", pd, "mp", peers)
        val sc = checkedStore(HogliteClient(standardUrl = "http://127.0.0.1:$pc", sleeper = {}))
        val sd = checkedStore(HogliteClient(standardUrl = "http://127.0.0.1:$pd", sleeper = {}))
        sc.createCatalog("mpcat", "s3://hoglake-it/mpcat")
        // The non-owner serves reads (proxied) AND the drop: hoglite routes
        // the release to the owner, and the delete proceeds once no live
        // owner remains.
        assertThat(sd.getCatalog("mpcat").name).isEqualTo("mpcat")
        sd.dropCatalog("mpcat", "p", "p")
        assertThat(sc.listCatalogs().map { it.name }).doesNotContain("mpcat")
        assertThatThrownBy { sc.getCatalog("mpcat") }.isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `multi-pod - a delete issued while the tenant is owned elsewhere is routed to the owner`() {
        val pc = ServerSocket(0).use { it.localPort }
        val pd = ServerSocket(0).use { it.localPort }
        val peers = "pod-e=http://127.0.0.1:$pc,pod-f=http://127.0.0.1:$pd"
        startPod("pod-e", pc, "mp", peers)
        startPod("pod-f", pd, "mp", peers)
        val ce = HogliteClient(standardUrl = "http://127.0.0.1:$pc", sleeper = {})
        val cf = HogliteClient(standardUrl = "http://127.0.0.1:$pd", sleeper = {})
        // pod-e creates (and owns) the tenant. The delete lands on pod-f:
        // hoglite proxies it to the owner, which stops its own actor and
        // purges — pre-routing this was a flat 409.
        ce.createTenant("routed-delete")
        cf.deleteTenant("routed-delete")
        // The deletion executed at the owner: pod-e no longer serves it.
        assertThatThrownBy {
            ce.query("routed-delete", "SELECT 1", emptyList())
        }.isInstanceOf(HogliteRejected::class.java)
            .hasMessageContaining("404")
    }

    // ---- commit: uploads, dedup, timestamps ----------------------------

    @Test
    fun `an upload claim abandoned between pre-flight and tx stops the publication`() {
        val (c, _) = setup()
        val owner = UUID.randomUUID()
        val claim = store.uploads.claim(c, UUID.randomUUID(), owner, "s3://hoglake-it/$c/up", "data")
        hooks +=
            Hook({ m, p, b -> isTx(m, p) && claim.path in b && "INSERT INTO files" in b }, before = {
                store.uploads.abandon(c, owner, listOf(claim.path))
            })
        val r =
            runCatching {
                pstore.commit(
                    c,
                    CommitRequest(
                        idempotencyKey = owner,
                        appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(claim.path, 1, 10, 5)))),
                    ),
                )
            }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(n(c, "SELECT COUNT(*) FROM files WHERE path = ?", claim.path)).isEqualTo(0L)
    }

    @Test
    fun `a lost response inside one unkeyed commit still applies exactly once`() {
        val (c, _) = setup()
        // The APPLIED tx's response is replaced by a 503; the client retries
        // with the SAME txid and hoglite's dedup answers the replay.
        hooks += Hook({ m, p, b -> isTx(m, p) && "lost.parquet" in b }, shortCircuit = 503)
        val before = n(c, "SELECT COUNT(*) FROM files")
        val r = runCatching { pstore.commit(c, appendReq(c, "lost.parquet", key = null)) }
        assertThat(r.isSuccess).isTrue()
        assertThat(n(c, "SELECT COUNT(*) FROM files") - before).isEqualTo(1L)
    }

    @Test
    fun `a same-key different-payload race is the clean typed 422`() {
        val (c, _) = setup()
        val key = UUID.randomUUID()
        hooks +=
            Hook({ m, p, b -> isTx(m, p) && "payloadA" in b }, before = {
                store.commit(c, appendReq(c, "payloadB.parquet", key = key))
            })
        val race = runCatching { pstore.commit(c, appendReq(c, "payloadA.parquet", key = key)) }
        assertThat(race.exceptionOrNull()).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(race.exceptionOrNull()!!.message).doesNotContain("{")
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
    fun `a drop through a replica with a stale tenant cache must not take a live recreated catalog`() {
        val c = freshCatalog()
        pstore.getCatalog(c) // Replica X caches name -> T1 through the observable proxy.
        val oldTenant = pstore.resolveTenant(c)
        // Replica Y drops, recreates and populates the name.
        store.dropCatalog(c, "y", "drop")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.commit(c, appendReq(c, "live.parquet"))
        // Replica X must consult the pinned registry incarnation, never
        // its stale cache (even a successful 404 retry would mask that).
        seen.clear()
        val r = runCatching { pstore.dropCatalog(c, "x", "drop") }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        assertThat(seen).noneMatch { "/v1/t/$oldTenant/" in it }
        assertThat(store.listCatalogs().map { it.name }).contains(c)
        assertThat(store.listFiles(c, "ns", "t")).hasSize(1)
    }

    @Test
    fun `a replica with a stale tenant cache serves a live catalog`() {
        val d = freshCatalog()
        store.getCatalog(d)
        pstore.dropCatalog(d, "y", "drop")
        pstore.createCatalog(d, "s3://hoglake-it/$d")
        pstore.createNamespace(d, "ns")
        pstore.createTable(d, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val first = runCatching { store.commit(d, appendReq(d, "w1.parquet")) }
        val second = runCatching { store.commit(d, appendReq(d, "w2.parquet")) }
        assertThat(first.isSuccess).isTrue()
        assertThat(second.isSuccess).isTrue()
        assertThat(store.listFiles(d, "ns", "t")).hasSize(2)
    }

    @Test
    fun `a consumer registered below a protected sweep's planned floor keeps its history`() {
        val (c, t) = setup()
        repeat(3) { store.commit(c, appendReq(c, "consumer$it.parquet")) }
        HogliteOptions(store).patch(c, PatchField.Set(1L), consumerFloor = true, author = "test", message = "protect")
        ageSnapshots(c)
        val hook =
            Hook({ m, p, b -> isTx(m, p) && "UPDATE meta SET earliest_snapshot_id" in b }, before = {
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
            Hook({ m, p, b -> isTx(m, p) && "expired-read.parquet" in b }, before = {
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
            Hook({ m, p, b -> isTx(m, p) && "UPDATE uploads SET state = 'abandoned', scheduled_at" in b }, before = {
                assertThat(store.uploads.renew(c, owner)).isEqualTo(1)
            })
        hooks += hook
        assertThat(pstore.uploads.scheduleExpired(c, 100)).isZero()
        assertThat(hook.remaining).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM removals WHERE path = ?", claim.path)).isZero()
        assertThat(q(c, "SELECT state FROM uploads WHERE path = ?", claim.path).firstRow()!![0]).isEqualTo("active")
    }

    @Test
    fun `cleanup only deletes objects for removal rows it actually acquired`() {
        val c = freshCatalog()
        val key = "$c/unclaimed.parquet"
        val path = "s3://hoglake-it/$key"
        s3().use {
            it.putObject(
                { b -> b.bucket("hoglake-it").key(key) },
                software.amazon.awssdk.core.sync.RequestBody.fromString("keep"),
            )
        }
        boundTx(
            c,
            HogliteCatalogStore.freshTxid("queue"),
            null,
            HogliteTx().apply {
                add(
                    "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                        "VALUES (?, 'data', 'snapshot_expiry', 0, ?)",
                    path,
                    HogliteSchema.now(),
                )
            },
        )
        val hook =
            Hook({ m, p, b -> isTx(m, p) && "UPDATE removals SET claimed_at = ?, claimed_by = ?" in b }, before = {
                boundTx(
                    c,
                    HogliteCatalogStore.freshTxid("other-worker"),
                    null,
                    HogliteTx().apply {
                        add(
                            "UPDATE removals SET claimed_at = ?, claimed_by = 'other-worker' WHERE path = ?",
                            HogliteSchema.now(),
                            path,
                        )
                    },
                )
            })
        hooks += hook
        val cfg =
            com.posthog.hoglake.Config(
                database = com.posthog.hoglake.DatabaseBackend.HOGLITE,
                hogliteUrl = "http://127.0.0.1:1",
                s3Endpoint = s3Endpoint,
                s3Region = "us-east-1",
                s3AccessKey = minio.userName,
                s3SecretKey = minio.password,
                s3PathStyle = true,
            )
        HogliteCleanup(cfg, pstore).use { cleanup ->
            val result = cleanup.runOnce(c, 100, MaintenanceTrigger.MANUAL)
            assertThat(result.objectsRemoved).isZero()
            assertThat(result.removed + result.missing).isZero()
        }
        assertThat(hook.remaining).isZero()
        s3().use { assertThat(it.headObject { b -> b.bucket("hoglake-it").key(key) }.contentLength()).isEqualTo(4L) }
        assertThat(
            q(c, "SELECT claimed_by, drained_at FROM removals WHERE path = ?", path).firstRow(),
        ).containsExactly("other-worker", null)
    }

    @Test
    fun `a transaction with a stale incarnation fails without replay and evicts it`() {
        val c = freshCatalog()
        pstore.getCatalog(c)
        val oldTenant = pstore.resolveTenant(c)
        val consumerFloor = store.getCatalog(c).consumerFloor
        assertThatThrownBy {
            pstore.catalogOperation(c) {
                store.dropCatalog(c, "test", "old")
                store.createCatalog(c, "s3://hoglake-it/$c")
                seen.clear()
                pstore.runTx(
                    c,
                    HogliteCatalogStore.freshTxid("stale-write"),
                    null,
                    HogliteTx().apply {
                        add("UPDATE meta SET consumer_floor = ? WHERE id = 1", if (consumerFloor) 0 else 1)
                    },
                )
            }
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        val attempts = seen.filter { "/tx " in it }
        assertThat(attempts).hasSize(1)
        assertThat(attempts.single()).contains("/v1/t/$oldTenant/tx")
        assertThat(pstore.resolveTenant(c)).isEqualTo(store.resolveTenant(c))
        assertThat(store.getCatalog(c).consumerFloor).isEqualTo(consumerFloor)
    }

    @Test
    fun `a concurrent duplicate create reclaims only its own unpublished tenant`() {
        val c = "race${catalogCounter++}"
        var loserTenant = ""
        hooks +=
            Hook({ m, p, b ->
                if (isTx(m, p) && "bootstrap:v1" in b && "/hoglake-$c-" in p) {
                    loserTenant = p.substringAfter("/v1/t/").substringBefore("/tx")
                    true
                } else {
                    false
                }
            })
        val publish =
            Hook({ m, p, b -> isTx(m, p) && "INSERT INTO catalogs" in b && c in b }, before = {
                store.createCatalog(c, "s3://hoglake-it/$c")
            })
        hooks += publish
        assertThatThrownBy { pstore.createCatalog(c, "s3://hoglake-it/$c") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        assertThat(publish.remaining).isZero()
        assertThat(loserTenant).isNotEmpty()
        assertThat(store.resolveTenant(c)).isNotEqualTo(loserTenant)
        assertThatThrownBy { client.query(loserTenant, "SELECT 1", emptyList()) }
            .isInstanceOf(HogliteRejected::class.java).hasMessageContaining("404")
        assertThat(store.getCatalog(c).name).isEqualTo(c)
    }

    @Test
    fun `stale-cache drop of an EMPTY recreated catalog drops the new incarnation, never touches the old id`() {
        val c = freshCatalog()
        pstore.getCatalog(c)
        val oldTenant = pstore.resolveTenant(c)
        store.dropCatalog(c, "y", "drop")
        store.createCatalog(c, "s3://hoglake-it/$c")
        val newTenant = store.resolveTenant(c)
        seen.clear()
        pstore.dropCatalog(c, "x", "drop")
        assertThat(seen).noneMatch { "/v1/t/$oldTenant/" in it }
        assertThat(store.listCatalogs().map { it.name }).doesNotContain(c)
        assertThatThrownBy { client.query(newTenant, "SELECT 1", emptyList()) }
            .isInstanceOf(HogliteRejected::class.java).hasMessageContaining("404")
    }

    @Test
    fun `stale-cache read after a remote drop (no recreate) is NotFound, and a recreate is then served`() {
        val c = freshCatalog()
        pstore.getCatalog(c)
        store.dropCatalog(c, "y", "drop")
        val r = runCatching { pstore.getCatalog(c) }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        store.createCatalog(c, "s3://hoglake-it/$c")
        assertThat(pstore.getCatalog(c).name).isEqualTo(c)
    }

    @Test
    fun `stale-cache drop where the pinned incarnation is a real ghost still clears the row`() {
        val c = freshCatalog()
        pstore.getCatalog(c) // X caches T1
        val t1 = pstore.resolveTenant(c)
        // Simulate a drop that died after deleting the tenant but before the registry delete.
        client.release(t1)
        client.deleteTenant(t1)
        val r = runCatching { pstore.dropCatalog(c, "x", "drop") }
        assertThat(r.isSuccess).isTrue()
        assertThat(store.listCatalogs().map { it.name }).doesNotContain(c)
    }

    @Test
    fun `a commit planned on the old incarnation must not be retried into a recreated catalog`() {
        val (c, t) = setup() // T1: ns.t is table id 1, no files
        val oldTenant = store.resolveTenant(c)
        val hook =
            Hook({ m, p, b -> isTx(m, p) && "xinc.parquet" in b && "/v1/t/$oldTenant/" in p }, before = {
                // While the commit's tx is in flight (planned against T1):
                // the table, namespace and catalog go away, and the name is
                // recreated with an unrelated first table (id 1 again).
                store.dropTable(c, "ns", "t", t.tableUuid, "p", "p")
                store.dropNamespace(c, "ns", null, "p", "p")
                store.dropCatalog(c, "y", "drop")
                store.createCatalog(c, "s3://hoglake-it/$c")
                store.createNamespace(c, "zz")
                store.createTable(c, "zz", "victim", listOf(ColumnDef("id", ColType.LONG)))
            })
        hooks += hook
        val r = runCatching { pstore.commit(c, appendReq(c, "xinc.parquet")) }
        val victimFiles = store.listFiles(c, "zz", "victim")
        assertThat(hook.remaining).isZero()
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(victimFiles).isEmpty()
        val attempts = seen.filter { "/tx " in it && "xinc.parquet" in it }
        assertThat(attempts).hasSize(1)
        assertThat(attempts.single()).contains("/v1/t/$oldTenant/tx")
    }

    @Test
    fun `a ghost drop racing a recreate leaves the new row alone`() {
        val c = freshCatalog()
        val t1 = store.resolveTenant(c)
        client.release(t1)
        client.deleteTenant(t1) // ghost: registry row -> T1, T1 gone
        val hook =
            Hook({ m, p, b -> "/v1/t/$t1/query" in p && "FROM meta" in b }, before = {
                // between X's registry read and its meta probe: another
                // replica clears the ghost and recreates the name
                store.dropCatalog(c, "y", "drop")
                store.createCatalog(c, "s3://hoglake-it/$c")
            })
        hooks += hook
        val r = runCatching { pstore.dropCatalog(c, "x", "drop") }
        assertThat(hook.remaining).isZero()
        assertThat(r.isSuccess).isTrue()
        assertThat(store.listCatalogs().map { it.name }).contains(c)
        assertThat(store.getCatalog(c).name).isEqualTo(c)
    }

    private fun recreateWithVictim(
        s: HogliteCatalogStore,
        c: String,
        t: com.posthog.hoglake.model.TableInfo,
    ) {
        s.dropTable(c, "ns", "t", t.tableUuid, "p", "p")
        s.dropNamespace(c, "ns", null, "p", "p")
        s.dropCatalog(c, "y", "drop")
        s.createCatalog(c, "s3://hoglake-it/$c")
        s.createNamespace(c, "zz")
        s.createTable(c, "zz", "victim", listOf(ColumnDef("id", ColType.LONG)))
    }

    @Test
    fun `a planning read cannot switch the incarnation of an existing commit plan`() {
        val (c, t) = setup()
        val oldTenant = store.resolveTenant(c)
        pstore.getCatalog(c)
        val hook =
            Hook({ m, p, b -> m == "POST" && "/v1/t/$oldTenant/query" in p && "partition_specs" in b }, before = {
                recreateWithVictim(store, c, t)
            })
        hooks += hook
        seen.clear()
        val r = runCatching { pstore.commit(c, appendReq(c, "p5a.parquet")) }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(checkedStore(client).listFiles(c, "zz", "victim")).isEmpty()
    }

    @Test
    fun `a same-replica recreate cannot retarget an existing commit plan`() {
        val (c, t) = setup()
        val oldTenant = store.resolveTenant(c)
        pstore.getCatalog(c)
        val hook =
            Hook({ m, p, b -> m == "POST" && "/v1/t/$oldTenant/query" in p && "partition_specs" in b }, after = {
                recreateWithVictim(pstore, c, t)
            })
        hooks += hook
        seen.clear()
        val r = runCatching { pstore.commit(c, appendReq(c, "p5b.parquet")) }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(checkedStore(client).listFiles(c, "zz", "victim")).isEmpty()
    }

    @Test
    fun `a concurrent cache refresh cannot retarget an existing commit plan`() {
        val (c, t) = setup()
        val oldTenant = store.resolveTenant(c)
        pstore.getCatalog(c)
        val hook =
            Hook({ m, p, b -> m == "POST" && "/v1/t/$oldTenant/query" in p && "partition_specs" in b }, after = {
                recreateWithVictim(store, c, t)
                pstore.getCatalog(c)
            })
        hooks += hook
        seen.clear()
        val r = runCatching { pstore.commit(c, appendReq(c, "p5c.parquet")) }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(checkedStore(client).listFiles(c, "zz", "victim")).isEmpty()
    }

    @Test
    fun `a partition-spec planning read cannot move a commit into a replacement tenant`() {
        val (c, t) = setup()
        val oldTenant = store.resolveTenant(c)
        val hook =
            Hook({ _, p, b ->
                "/v1/t/$oldTenant/query" in p && "SELECT spec_id, fields_json FROM partition_specs" in b
            }, before = {
                store.dropTable(c, "ns", "t", t.tableUuid, "p", "p")
                store.dropNamespace(c, "ns", null, "p", "p")
                store.dropCatalog(c, "y", "drop")
                store.createCatalog(c, "s3://hoglake-it/$c")
                store.createNamespace(c, "zz")
                store.createTable(c, "zz", "victim", listOf(ColumnDef("id", ColType.LONG)))
            })
        hooks += hook
        val result = runCatching { pstore.commit(c, appendReq(c, "read-incarnation.parquet")) }
        val files = store.listFiles(c, "zz", "victim")
        assertThat(hook.remaining).isZero()
        assertThat(files).describedAs("a read refresh must not redirect an existing table-id plan").isEmpty()
        assertThat(result.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `a sibling refresh after the upload planning read cannot retarget a commit`() {
        val (c, t) = setup()
        val oldTenant = store.resolveTenant(c)
        val hook =
            Hook({ _, p, b ->
                "/v1/t/$oldTenant/query" in p && "SELECT upload_id, path, file_kind, state, owner FROM uploads" in b
            }, after = {
                // This planning read has ALREADY returned from T1. A sibling
                // request on this same store refreshes its shared name cache
                // before the committing caller gets the old read response.
                store.dropTable(c, "ns", "t", t.tableUuid, "p", "p")
                store.dropNamespace(c, "ns", null, "p", "p")
                store.dropCatalog(c, "y", "drop")
                store.createCatalog(c, "s3://hoglake-it/$c")
                store.createNamespace(c, "zz")
                store.createTable(c, "zz", "victim", listOf(ColumnDef("id", ColType.LONG)))
                pstore.getCatalog(c)
            })
        hooks += hook
        val result = runCatching { pstore.commit(c, appendReq(c, "sibling-incarnation.parquet")) }
        val files = store.listFiles(c, "zz", "victim")
        assertThat(hook.remaining).isZero()
        assertThat(files).describedAs("an unrelated request's cache refresh must not retarget this plan").isEmpty()
        assertThat(result.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
    }

    private fun recreateEmptyAndRefresh(c: String) {
        store.dropCatalog(c, "test", "recreate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        pstore.getCatalog(c)
    }

    @Test
    fun `an options patch cannot cross incarnations after reading its policy`() {
        val c = freshCatalog()
        val old = store.resolveTenant(c)
        val hook =
            Hook({ _, p, b -> "/v1/t/$old/query" in p && "FROM meta" in b }, after = {
                recreateEmptyAndRefresh(c)
            })
        hooks += hook
        assertThatThrownBy {
            HogliteOptions(pstore).patch(c, PatchField.Absent, false, "test", "policy")
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).consumerFloor).isTrue()
    }

    @Test
    fun `an upload claim cannot cross incarnations after checking its data path`() {
        val c = freshCatalog()
        val old = store.resolveTenant(c)
        val hook =
            Hook({ _, p, b -> "/v1/t/$old/query" in p && "FROM meta" in b }, after = {
                recreateEmptyAndRefresh(c)
            })
        hooks += hook
        assertThatThrownBy {
            pstore.uploads.claim(c, UUID.randomUUID(), UUID.randomUUID(), "s3://hoglake-it/$c/up", "data")
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM uploads")).isZero()
    }

    @Test
    fun `an expiry sweep and its run ledger stay on their entry incarnation`() {
        val c = freshCatalog()
        val old = store.resolveTenant(c)
        val hook =
            Hook({ _, p, b -> "/v1/t/$old/query" in p && "FROM meta" in b }, after = {
                recreateEmptyAndRefresh(c)
            })
        hooks += hook
        assertThatThrownBy {
            HogliteExpiry(pstore, 100, 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL)
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM maintenance_runs")).isZero()
        assertThat(store.getCatalog(c).earliestSnapshotId).isZero()
    }

    @Test
    fun `a create-table plan cannot reuse a namespace id in a replacement tenant`() {
        val c = freshCatalog()
        store.createNamespace(c, "ns")
        val old = store.resolveTenant(c)
        val hook =
            Hook({ _, p, b -> "/v1/t/$old/query" in p && "FROM namespaces WHERE name" in b }, after = {
                store.dropNamespace(c, "ns", null, "test", "drop")
                store.dropCatalog(c, "test", "drop")
                store.createCatalog(c, "s3://hoglake-it/$c")
                store.createNamespace(c, "zz")
                pstore.getCatalog(c)
            })
        hooks += hook
        assertThatThrownBy {
            pstore.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(n(c, "SELECT COUNT(*) FROM tables")).isZero()
    }

    @Test
    fun `a swallowed bound 404 still fails the operation without writing a replacement run ledger`() {
        val c = freshCatalog()
        val old = store.resolveTenant(c)
        assertThatThrownBy {
            pstore.recorded(c, com.posthog.hoglake.model.MaintenanceTask.EXPIRY, MaintenanceTrigger.MANUAL) {
                pstore.meta(c)
                client.release(old)
                client.deleteTenant(old)
                assertThat(runCatching { pstore.queryCatalog(c, "SELECT 1") }.exceptionOrNull())
                    .isInstanceOf(HoglakeException.NotFound::class.java)
                // Even a broad catch must not permit the recorded boundary to
                // acknowledge success or write its ledger to the new tenant.
                val sibling = Executors.newSingleThreadExecutor()
                try {
                    sibling.submit { recreateEmptyAndRefresh(c) }.get(10, TimeUnit.SECONDS)
                } finally {
                    sibling.shutdownNow()
                }
                42
            }
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(n(c, "SELECT COUNT(*) FROM maintenance_runs")).isZero()
        // Finally unwinds the poisoned scope; a NEW operation may bind T2.
        assertThat(pstore.getCatalog(c).name).isEqualTo(c)
    }

    @Test
    fun `catalog bootstrap readback cannot acknowledge another incarnation`() {
        val c = "race${catalogCounter++}"
        val hook =
            Hook({ m, _, b -> m == "POST" && "INSERT INTO catalogs" in b && c in b }, after = {
                recreateEmptyAndRefresh(c)
            })
        hooks += hook
        assertThatThrownBy {
            pstore.createCatalog(c, "s3://hoglake-it/$c")
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getCatalog(c).name).isEqualTo(c)
    }

    /** `ns.t(id)` partitioned by identity(id), optionally holding one file for id=7. */
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

    private fun recreatePartitioned(
        c: String,
        t: com.posthog.hoglake.model.TableInfo,
        path: String?,
    ) {
        store.dropTable(c, "ns", "t", t.tableUuid, "p", "p")
        store.dropNamespace(c, "ns", null, "p", "p")
        store.dropCatalog(c, "y", "drop")
        store.createCatalog(c, "s3://hoglake-it/$c")
        partitionedWithFile(c, path)
        pstore.getCatalog(c)
    }

    @Test
    fun `a partition lookup cannot answer from a replacement tenant after resolving its table`() {
        val c = freshCatalog()
        val t = partitionedWithFile(c, null)
        val old = store.resolveTenant(c)
        pstore.getCatalog(c)
        // After the lookup has resolved the table (and its id) on the old
        // incarnation, where the partition is empty, the name is recreated
        // with the SAME shape and ids plus a file for the value, and this
        // replica's cache is refreshed to it. Answering would report the
        // replacement's file as the old table's partition.
        val hook =
            Hook({ m, p, b -> m == "POST" && "/v1/t/$old/query" in p && "next_file_id" in b }, after = {
                recreatePartitioned(c, t, "victim7.parquet")
            })
        hooks += hook
        val r = runCatching { HoglitePartitionLookup(pstore).lookup(c, "ns", "t", 1, listOf("7")) }
        assertThat(r.exceptionOrNull()).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        // The fresh incarnation is served on a fresh entry.
        val fresh = HoglitePartitionLookup(pstore).lookup(c, "ns", "t", 1, listOf("7"))
        assertThat(fresh.files.map { it.path }).containsExactly("s3://hoglake-it/$c/data/victim7.parquet")
    }

    @Test
    fun `set_object_prefix_field cannot cross incarnations after its emptiness probe`() {
        val c = freshCatalog()
        val t = partitionedWithFile(c, null)
        val old = store.resolveTenant(c)
        pstore.getCatalog(c)
        val hook =
            Hook({ m, p, b -> m == "POST" && "/v1/t/$old/query" in p && "FROM dvs" in b }, after = {
                recreatePartitioned(c, t, null)
            })
        hooks += hook
        assertThatThrownBy {
            HogliteAlters(pstore).alterTable(c, "ns", "t", listOf(AlterOp.SetObjectPrefixField("id")))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(hook.remaining).isZero()
        assertThat(store.getTable(c, "ns", "t").objectPrefixField).isNull()
        assertThat(n(c, "SELECT COUNT(*) FROM table_layouts")).isZero()
    }

    /** Raw fixture SQL still enters an explicit scope; never exempt production work. */
    private fun checkedStore(client: HogliteClient) =
        HogliteCatalogStore(client).apply { requireOperationBindingForTests = true }

    private fun boundTx(
        c: String,
        txid: String,
        fp: String?,
        tx: HogliteTx,
    ) = store.catalogOperation(c) { store.runTx(c, txid, fp, tx) }

    @Suppress("UNCHECKED_CAST")
    private fun bindings(s: HogliteCatalogStore): Map<String, Any>? {
        val field = HogliteCatalogStore::class.java.getDeclaredField("operationBindings")
        field.isAccessible = true
        return (field.get(s) as ThreadLocal<MutableMap<String, Any>>).get()
    }

    private fun victimAppend(
        c: String,
        path: String,
    ) = CommitRequest(
        idempotencyKey = UUID.randomUUID(),
        appends =
            listOf(
                TableAppend("zz", "victim", listOf(FileRegistration("s3://hoglake-it/$c/data/$path", 10, 100, 50))),
            ),
    )

    @Test
    fun `every catch-and-retry after a bound 404 rethrows the same NotFound and sends nothing`() {
        val (c, t) = setup()
        var first: Throwable? = null
        val attempts = linkedMapOf<String, Throwable?>()
        var sentAfterPoison = -1
        val r =
            runCatching {
                pstore.catalogOperation(c) {
                    pstore.meta(c) // bound to T1
                    recreateWithVictim(store, c, t) // T2 holds zz.victim (table id 1).
                    first = runCatching { pstore.queryCatalog(c, "SELECT 1") }.exceptionOrNull()
                    val before = seen.size

                    fun attempt(
                        name: String,
                        body: () -> Unit,
                    ) {
                        attempts[name] = runCatching(body).exceptionOrNull()
                    }
                    attempt("commit ns.t") { pstore.commit(c, appendReq(c, "poison.parquet")) }
                    attempt("commit zz.victim") { pstore.commit(c, victimAppend(c, "poison-victim.parquet")) }
                    attempt("createNamespace") { pstore.createNamespace(c, "poison") }
                    attempt(
                        "createTable",
                    ) { pstore.createTable(c, "zz", "poison", listOf(ColumnDef("id", ColType.LONG))) }
                    attempt("runTx") {
                        val tx = HogliteTx().apply { add("UPDATE meta SET consumer_floor = 0 WHERE id = 1") }
                        pstore.runTx(c, "poison", null, tx)
                    }
                    attempt("queryCatalog") { pstore.queryCatalog(c, "SELECT COUNT(*) FROM tables") }
                    attempt("getCatalog") { pstore.getCatalog(c) }
                    attempt(
                        "options.patch",
                    ) { HogliteOptions(pstore).patch(c, PatchField.Absent, false, "poison", "poison") }
                    attempt("uploads.claim") {
                        pstore.uploads.claim(c, UUID.randomUUID(), UUID.randomUUID(), "s3://hoglake-it/$c/up", "data")
                    }
                    attempt("recorded{ok}") {
                        pstore.recorded(
                            c,
                            com.posthog.hoglake.model.MaintenanceTask.EXPIRY,
                            MaintenanceTrigger.MANUAL,
                        ) { 1 }
                    }
                    attempt(
                        "expiry.runOnce",
                    ) { HogliteExpiry(pstore, 100, 10_000).runOnce(c, 100, MaintenanceTrigger.MANUAL) }
                    sentAfterPoison = seen.size - before
                    42 // the broad catch "succeeds"
                }
            }
        assertThat(first).isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(attempts).hasSize(11)
        attempts.forEach { (k, v) -> assertThat(v).describedAs(k).isSameAs(first) }
        assertThat(sentAfterPoison).isZero()
        assertThat(r.exceptionOrNull()).isSameAs(first)
        assertThat(bindings(pstore)).isNull()
        val t2 = checkedStore(client)
        assertThat(t2.listFiles(c, "zz", "victim")).isEmpty()
        assertThat(t2.listTables(c, "zz", null, null).map { it.name }).containsExactly("victim")
        assertThat(t2.listNamespaces(c, null, null).map { it.name }).containsExactly("zz")
        assertThat(
            t2.catalogOperation(c) {
                t2.queryCatalog(c, "SELECT COUNT(*) FROM maintenance_runs")
            }.firstRow()!![0].asLong(),
        ).isZero()
        assertThat(
            t2.catalogOperation(c) {
                t2.queryCatalog(c, "SELECT COUNT(*) FROM uploads")
            }.firstRow()!![0].asLong(),
        ).isZero()
        assertThat(t2.getCatalog(c).consumerFloor).isTrue() // default 1; poisoned retries tried to clear it
        // A NEW operation on the same thread rebinds authoritatively and is served by T2.
        pstore.commit(c, victimAppend(c, "after-unwind.parquet"))
        assertThat(
            t2.listFiles(c, "zz", "victim").map {
                it.path
            },
        ).containsExactly("s3://hoglake-it/$c/data/after-unwind.parquet")
    }

    @Test
    fun `a fence-poisoned scope stays poisoned even if the old tenant would accept the retry`() {
        val c = freshCatalog()
        val old = store.resolveTenant(c)
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
        // and a new operation (old tenant still registered, now unfenced) proceeds
        assertThat(pstore.createNamespace(c, "a").name).isEqualTo("a")
    }
}
