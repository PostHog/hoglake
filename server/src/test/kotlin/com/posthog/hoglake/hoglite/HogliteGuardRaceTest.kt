package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
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
        store = HogliteCatalogStore(client)

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
                respond(resp.statusCode(), resp.body())
            } catch (e: Throwable) {
                ex.sendResponseHeaders(599, -1)
                ex.close()
            }
        }
        proxy.start()
        pstore =
            HogliteCatalogStore(
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
    ) = store.queryCatalog(c, sql, *a)

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
        store.runTx(
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
        val sc = HogliteCatalogStore(HogliteClient(standardUrl = "http://127.0.0.1:$pc", sleeper = {}))
        val sd = HogliteCatalogStore(HogliteClient(standardUrl = "http://127.0.0.1:$pd", sleeper = {}))
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
        store.runTx(
            c,
            HogliteCatalogStore.freshTxid("ts"),
            null,
            HogliteTx().apply { add("UPDATE snapshots SET ts = '2031-01-01T00:00:00.000Z' WHERE id = ${head(c)}") },
        )
        val r = store.commit(c, appendReq(c, "ts1.parquet"))
        val ts = q(c, "SELECT ts FROM snapshots WHERE id = ?", r.snapshotId).firstRow()!![0].asString()
        assertThat(ts).isGreaterThanOrEqualTo("2031-01-01T00:00:00.000Z")
    }
}
