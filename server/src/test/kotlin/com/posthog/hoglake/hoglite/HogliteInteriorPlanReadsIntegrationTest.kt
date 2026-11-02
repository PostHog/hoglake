package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.hogliteCatalogStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import com.sun.net.httpserver.HttpServer
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.io.File
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

/**
 * Interior plan reads ([HogliteCommit], "Interior plan reads") against a
 * real `hoglited -interior-reads`, through a relay that records every
 * request and can rewrite a write's incarnation token.
 *
 * Durability is stalled for real by pausing the MinIO container: the
 * owner applies a commit, its segment PUT hangs, and the commit is applied
 * but not durable. That is the window an interior read exists for, and
 * the one in which every other read must still park.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteInteriorPlanReadsIntegrationTest {
    private lateinit var registry: PrometheusMeterRegistry
    private lateinit var minio: MinIOContainer
    private var process: Process? = null
    private lateinit var binary: String
    private lateinit var relay: HttpServer
    private lateinit var s3Endpoint: String
    private var enginePort = 0

    @Volatile
    private var engineUrl = ""
    private var catalogCounter = 0
    private val json = jacksonObjectMapper()
    private val upstream = HttpClient.newHttpClient()

    /** One request the relay forwarded: what was asked and what the engine answered. */
    private class Seen(
        val path: String,
        val body: Map<String, Any?>,
    ) {
        /** The engine's answer; 0 while the request is still waiting for one (or never got one). */
        @Volatile
        var status: Int = 0

        val txid get() = body["txid"] as? String
        val interior get() = body["interior"] == true
        val incarnation get() = body["incarnation"] as? String
        val isReadTx get() = path.endsWith("/tx") && txid?.startsWith(HogliteClient.READ_TXID_PREFIX) == true
        val isWriteTx get() = path.endsWith("/tx") && !isReadTx
    }

    private val seen = ConcurrentLinkedQueue<Seen>()

    /** Rewrites a write's incarnation token before forwarding (null: pass through). */
    @Volatile
    private var tokenRewrite: ((String) -> String?)? = null

    /** Drops `interior` before forwarding: the engine serves the read as an old one would. */
    @Volatile
    private var engineIgnoresInterior = false

    @BeforeAll
    fun start() {
        val b = System.getenv("HOGLITE_BINARY") ?: findOnPath("hoglited")
        assumeTrue(b != null) { "no hoglited binary (set HOGLITE_BINARY); skipping" }
        binary = b!!
        minio = TestImages.minio()
        minio.start()
        s3Endpoint = "http://127.0.0.1:${minio.firstMappedPort}"
        s3().use { it.createBucket { r -> r.bucket(BUCKET) } }
        enginePort = ServerSocket(0).use { it.localPort }
        startEngine()

        relay = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        relay.executor = java.util.concurrent.Executors.newCachedThreadPool()
        relay.createContext("/") { ex ->
            try {
                var body = ex.requestBody.readBytes()
                val path = ex.requestURI.toString()
                var decoded: Map<String, Any?> = emptyMap()
                if (body.isNotEmpty() && ex.requestMethod == "POST") {
                    decoded = runCatching { json.readValue<Map<String, Any?>>(body) }.getOrDefault(emptyMap())
                    val token = decoded["incarnation"] as? String
                    val rewritten = token?.let { tokenRewrite?.invoke(it) }
                    if (rewritten != null) {
                        decoded = decoded + ("incarnation" to rewritten)
                        body = json.writeValueAsBytes(decoded)
                    }
                    if (engineIgnoresInterior && decoded.containsKey("interior")) {
                        body = json.writeValueAsBytes(decoded - "interior")
                    }
                }
                val rec = Seen(path, decoded).also { seen += it }
                val resp =
                    upstream.send(
                        HttpRequest.newBuilder(URI.create(engineUrl + path))
                            .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body))
                            .header("Content-Type", "application/json")
                            .build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                rec.status = resp.statusCode()
                val answer = resp.body()
                ex.sendResponseHeaders(resp.statusCode(), if (answer.isEmpty()) -1 else answer.size.toLong())
                if (answer.isNotEmpty()) ex.responseBody.use { it.write(answer) } else ex.close()
            } catch (e: Throwable) {
                // the engine went away mid-request (fail-stop test): a transport failure for the client
                ex.close()
            }
        }
        relay.start()
    }

    private fun startEngine() {
        val dir = Files.createTempDirectory("hoglited-interior").toFile()
        val builder =
            ProcessBuilder(
                binary, "-addr", "127.0.0.1:$enginePort", "-dir", dir.absolutePath,
                "-bucket", BUCKET, "-id", "interior-1", "-class", "standard",
                "-interior-reads",
            ).redirectErrorStream(true).redirectOutput(File(dir, "hoglited.log"))
        builder.environment().putAll(
            mapOf(
                "AWS_ENDPOINT_URL" to s3Endpoint,
                "AWS_ACCESS_KEY_ID" to minio.userName,
                "AWS_SECRET_ACCESS_KEY" to minio.password,
                "AWS_REGION" to "us-east-1",
            ),
        )
        process = builder.start()
        engineUrl = "http://127.0.0.1:$enginePort"
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (true) {
            val up =
                runCatching {
                    upstream.send(
                        HttpRequest.newBuilder(URI.create("$engineUrl/v1/healthz")).build(),
                        HttpResponse.BodyHandlers.discarding(),
                    ).statusCode() == 200
                }.getOrDefault(false)
            if (up) break
            if (System.nanoTime() >= deadline || !process!!.isAlive) {
                throw AssertionError(engineStartFailure(File(dir, "hoglited.log").readText()))
            }
            Thread.sleep(100)
        }
    }

    private fun engineStartFailure(log: String): String =
        "hoglited ($binary) did not come up; this suite requires -interior-reads support. Its log ends:\n" +
            log.lines().takeLast(20).joinToString("\n")

    @BeforeEach
    fun bindMetrics() {
        registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
    }

    @AfterEach
    fun reset() {
        tokenRewrite = null
        engineIgnoresInterior = false
        unpause()
        seen.clear()
        Metrics.clear()
        registry.close()
    }

    @AfterAll
    fun stop() {
        if (::relay.isInitialized) relay.stop(0)
        process?.destroy()
        process?.waitFor()
        if (::minio.isInitialized) {
            unpause()
            minio.stop()
        }
    }

    // ---- helpers ------------------------------------------------------

    private var paused = false

    /** Stall every S3 call of the engine: segment PUTs hang, so applied commits stop becoming durable. */
    private fun pause() {
        minio.dockerClient.pauseContainerCmd(minio.containerId).exec()
        paused = true
    }

    private fun unpause() {
        if (!paused) return
        minio.dockerClient.unpauseContainerCmd(minio.containerId).exec()
        paused = false
    }

    private fun store(
        interior: Boolean = true,
        budgetMs: Long = 30_000,
    ): HogliteCatalogStore =
        HogliteCatalogStore(
            HogliteClient(
                standardUrl = "http://127.0.0.1:${relay.address.port}",
                retryBudgetMs = budgetMs,
                // the fail-stop test retries through an engine restart
                circuitFailureThreshold = 1_000_000,
                sleeper = { Thread.sleep(minOf(it, 200)) },
            ),
            interiorPlanReads = interior,
        ).apply { requireOperationBindingForTests = true }

    private fun setup(store: HogliteCatalogStore): String {
        val c = "interior${catalogCounter++}"
        store.createCatalog(c, "s3://$BUCKET/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        seen.clear()
        return c
    }

    private fun append(
        c: String,
        file: String,
        key: UUID = UUID.randomUUID(),
    ) = CommitRequest(
        readSnapshot = null,
        idempotencyKey = key,
        appends = listOf(TableAppend("ns", "t", listOf(FileRegistration("s3://$BUCKET/$c/data/$file", 10, 100, 50)))),
    )

    private fun files(
        store: HogliteCatalogStore,
        c: String,
    ): List<String> = store.listFiles(c, "ns", "t", null, null, null, false, null, 0, emptyMap()).map { it.path }

    private fun unserved(registry: PrometheusMeterRegistry): Double =
        registry.find("hoglake_hoglite_interior_plan_reads_unserved_total").counter()?.count() ?: 0.0

    private fun metric(name: String): Long {
        val text =
            upstream.send(
                HttpRequest.newBuilder(URI.create("$engineUrl/metrics")).build(),
                HttpResponse.BodyHandlers.ofString(),
            ).body()
        return text.lineSequence().firstOrNull { it.startsWith("$name ") }?.substringAfter(' ')?.trim()?.toLong() ?: 0L
    }

    private fun <T> async(f: () -> T): CompletableFuture<T> = CompletableFuture.supplyAsync(f)

    /** Wait until the relay forwarded a request matching [p] (it may still be waiting for its answer). */
    private fun awaitSent(
        what: String,
        from: Int = 0,
        p: (Seen) -> Boolean,
    ): Seen {
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        while (System.nanoTime() < deadline) {
            seen.drop(from).firstOrNull(p)?.let { return it }
            Thread.sleep(20)
        }
        error("relay never saw $what; saw ${seen.map { it.path + " " + it.txid }}")
    }

    private fun writeOf(fileMarker: String) =
        { s: Seen -> s.isWriteTx && fileMarker in json.writeValueAsString(s.body) }

    // ---- tests --------------------------------------------------------

    @Test
    fun `only the commit plan read is interior and its write carries the token`() {
        val store = store()
        val c = setup(store)
        val unservedBefore = unserved(registry)
        val interiorBefore = metric("hoglite_interior_reads_total")
        val r1 = store.commit(c, append(c, "a.parquet"), null)
        val r2 = store.commit(c, append(c, "b.parquet"), null)
        assertThat(r2.snapshotId).isGreaterThan(r1.snapshotId)
        // the reads whose results reach a caller
        store.getTable(c, "ns", "t", null, null, false)
        assertThat(files(store, c)).hasSize(2)
        store.planScan(c, "ns", "t", null, null, null, false)
        store.changes(c, "ns", "t", 0, null)
        store.listNamespaces(c, null, null)
        store.getCatalog(c)

        val interior = seen.filter { it.interior }
        assertThat(interior).hasSize(2).allMatch { it.isReadTx && it.status == 200 }
        // every request that is not a plan read is an ordinary (parked) one
        assertThat(seen.filter { !it.interior && it.isReadTx }).isEmpty()
        val writes = seen.filter { it.isWriteTx }
        assertThat(writes).hasSize(2).allMatch { it.incarnation != null && it.status == 200 }
        assertThat(writes.map { it.incarnation }.toSet()).hasSize(1)
        assertThat(metric("hoglite_interior_reads_total") - interiorBefore).isEqualTo(2)
        assertThat(store.interiorRedecided.get()).isZero()
        assertThat(unserved(registry) - unservedBefore).isZero()
    }

    @Test
    fun `with the option off no read is interior and no write is fenced`() {
        val store = store(interior = false)
        val c = setup(store)
        val interiorBefore = metric("hoglite_interior_reads_total")
        store.commit(c, append(c, "a.parquet"), null)
        assertThat(seen.filter { it.interior }).isEmpty()
        assertThat(seen.filter { it.isWriteTx }).hasSize(1).allMatch { it.incarnation == null }
        assertThat(metric("hoglite_interior_reads_total") - interiorBefore).isZero()
    }

    @Test
    fun `the setting on against an engine that does not serve interior reads is counted, and commits stay correct`() {
        // The pairing an operator can get wrong: HOGLAKE_HOGLITE_INTERIOR_PLAN_READS
        // on, the engine too old or without -interior-reads. It parks the
        // read and returns no token. The commit must still succeed on that
        // (durable) plan, with no fenced write, and the mismatch must be
        // counted rather than silent.
        // MUTATION: drop the `incarnation == null` check in
        // readCatalogInterior and the count stays zero.
        val store = store()
        val c = setup(store)
        engineIgnoresInterior = true
        val unservedBefore = unserved(registry)
        val interiorBefore = metric("hoglite_interior_reads_total")
        val r1 = store.commit(c, append(c, "a.parquet"), null)
        val r2 = store.commit(c, append(c, "b.parquet"), null)
        assertThat(r2.snapshotId).isGreaterThan(r1.snapshotId)
        assertThat(seen.filter { it.interior }).hasSize(2)
        assertThat(seen.filter { it.isWriteTx }).hasSize(2).allMatch { it.incarnation == null && it.status == 200 }
        assertThat(metric("hoglite_interior_reads_total") - interiorBefore).isZero()
        assertThat(unserved(registry) - unservedBefore).isEqualTo(2.0)
        assertThat(registry.scrape()).contains("hoglake_hoglite_interior_plan_reads_unserved_total 2.0")
        assertThat(store.interiorRedecided.get()).isZero()
    }

    @Test
    fun `mixed boot factory enables only hoglite commit plans and leaves postgres and registry reads durable`() {
        PgTestSupport.freshDatabase().use { db ->
            for (enabled in listOf(false, true)) {
                val cfg =
                    Config(
                        database = DatabaseBackend.MIXED,
                        hogliteUrl = "http://127.0.0.1:${relay.address.port}",
                        hogliteInteriorPlanReads = enabled,
                    )
                HogliteClient.fromConfig(cfg).use { client ->
                    // This is the constructor Main uses for BOTH hoglite-capable boot paths.
                    val store = hogliteCatalogStore(cfg, client)
                    App.buildMixed(cfg, db.jdbi, store, HealthProbe.forProbe(1_000) { true }).use { app ->
                        val hg = "interior${catalogCounter++}"
                        val pg = "interior${catalogCounter++}"
                        for ((c, backend) in listOf(hg to "hoglite", pg to "postgres")) {
                            app.store.createCatalog(c, "s3://$BUCKET/$c", backend = backend)
                            app.store.createNamespace(c, "ns")
                            app.store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                        }
                        seen.clear()
                        val before = metric("hoglite_interior_reads_total")
                        app.store.commit(hg, append(hg, "a.parquet"), null)
                        val plan = seen.filter { it.interior }
                        assertThat(plan).hasSize(if (enabled) 1 else 0)
                        assertThat(plan).allMatch { it.isReadTx && it.status == 200 }
                        assertThat(seen.filter { it.isWriteTx }).hasSize(1)
                            .allMatch { (it.incarnation != null) == enabled && it.status == 200 }
                        assertThat(metric("hoglite_interior_reads_total") - before).isEqualTo(if (enabled) 1L else 0L)
                        seen.clear()
                        val committed = app.store.commit(pg, append(pg, "a.parquet"), null)
                        assertThat(committed.snapshotId).isPositive()
                        app.store.getTable(hg, "ns", "t", null, null, false)
                        assertThat(app.store.listFiles(hg, "ns", "t", null, null, null, false, null, 0, emptyMap()))
                            .hasSize(1)
                        app.store.listCatalogs()
                        app.store.getCatalog(pg)
                        assertThat(seen.filter { it.path == "/v1/t/${store.registryTenantId}/query" })
                            .describedAs("mixed routing still consults the durable registry").isNotEmpty()
                        assertThat(seen.filter { it.interior || it.isWriteTx }).isEmpty()
                        assertThat(metric("hoglite_interior_reads_total") - before).isEqualTo(if (enabled) 1L else 0L)
                    }
                }
            }
        }
    }

    @Test
    fun `during a durability stall the plan read answers, a caller read parks, the commit acks once durable`() {
        val store = store()
        val c = setup(store)
        val parkedBefore = metric("hoglite_parked_reads_total")
        pause()
        try {
            // A: applied by the owner, its segment PUT hangs — not durable
            val a = async { store.commit(c, append(c, "a.parquet"), null) }
            awaitSent("A's write", p = writeOf("a.parquet"))
            Thread.sleep(300)
            assertThat(a).isNotDone()
            // a read whose result reaches a caller parks behind A
            val table = async { files(store, c) }
            // C: its plan read sees A without waiting; its write waits for A
            val mark = seen.size
            val cResult = async { store.commit(c, append(c, "c.parquet"), null) }
            val cPlan = awaitSent("C's plan read answered during the stall", mark) { it.isReadTx && it.status == 200 }
            assertThat(cPlan.interior).isTrue()
            awaitSent("C's write", p = writeOf("c.parquet"))
            Thread.sleep(500)
            assertThat(table).describedAs("a caller read must park while A is not durable").isNotDone()
            assertThat(cResult).describedAs("C must not ack before A is durable").isNotDone()
            assertThat(a).isNotDone()
            unpause()
            val ra = a.get(20, TimeUnit.SECONDS)
            val rc = cResult.get(20, TimeUnit.SECONDS)
            assertThat(
                rc.snapshotId,
            ).describedAs("C was planned on a state that includes A").isGreaterThan(ra.snapshotId)
            assertThat(table.get(20, TimeUnit.SECONDS)).contains("s3://$BUCKET/$c/data/a.parquet")
        } finally {
            unpause()
        }
        assertThat(metric("hoglite_parked_reads_total") - parkedBefore).isGreaterThanOrEqualTo(1)
        assertThat(store.interiorRedecided.get()).isZero()
    }

    @Test
    fun `a write meeting another incarnation is redecided on a parked plan with the same txid`() {
        val store = store()
        val c = setup(store)
        var rewrites = 0
        tokenRewrite = { if (rewrites++ == 0) "interior-1/not-this-incarnation" else null }
        val r = store.commit(c, append(c, "a.parquet"), null)
        val writes = seen.filter { it.isWriteTx }
        assertThat(writes).hasSize(2)
        assertThat(writes[0].status).isEqualTo(412)
        assertThat(writes[1].status).isEqualTo(200)
        assertThat(writes[1].txid).isEqualTo(writes[0].txid)
        assertThat(writes[1].incarnation).describedAs("the redecided write is planned on a parked read").isNull()
        val reads = seen.filter { it.isReadTx }
        assertThat(reads.map { it.interior }).containsExactly(true, false)
        assertThat(store.interiorRedecided.get()).isEqualTo(1)
        assertThat(files(store, c)).containsExactly("s3://$BUCKET/$c/data/a.parquet")
        assertThat(store.getCatalog(c).headSnapshotId).isEqualTo(r.snapshotId)
    }

    @Test
    fun `a receipt replay seen by an interior plan is answered from a parked plan, never from the interior one`() {
        val store = store()
        val c = setup(store)
        val key = UUID.randomUUID()
        val first = store.commit(c, append(c, "a.parquet", key), null)
        seen.clear()
        val again = store.commit(c, append(c, "a.parquet", key), null)
        assertThat(again).isEqualTo(first)
        assertThat(seen.filter { it.isWriteTx }).describedAs("a replay writes nothing").isEmpty()
        assertThat(seen.filter { it.isReadTx }.map { it.interior }).containsExactly(true, false)
        assertThat(store.interiorRedecided.get()).isEqualTo(1)
    }

    @Test
    fun `a refusal decided on an interior plan is decided again on a parked one`() {
        val store = store()
        val c = setup(store)
        val err =
            catchThrowable {
                store.commit(
                    c,
                    CommitRequest(
                        readSnapshot = null,
                        idempotencyKey = UUID.randomUUID(),
                        appends =
                            listOf(
                                TableAppend(
                                    "ns",
                                    "missing",
                                    listOf(FileRegistration("s3://$BUCKET/$c/data/x.parquet", 1, 1, 1)),
                                ),
                            ),
                    ),
                    null,
                )
            }
        assertThat(err).isInstanceOf(HoglakeException::class.java)
        assertThat(seen.filter { it.isWriteTx }).isEmpty()
        assertThat(seen.filter { it.isReadTx && it.interior }).hasSize(1)
        assertThat(seen.filter { it.isReadTx && !it.interior }).isNotEmpty()
        assertThat(store.interiorRedecided.get()).isEqualTo(1)
    }

    /**
     * The ordering claim end to end: A is applied but not durable, C's
     * interior plan sees A and C's fenced write is queued behind it — then
     * the owner dies. Neither may have been acknowledged by the dead
     * owner; after recovery each commit acks exactly once, from state the
     * new owner holds, whichever of them the dead owner's buffered PUTs
     * managed to make durable.
     */
    @Test
    fun `fail-stop with an interior-planned commit queued behind a non-durable one acks nothing from lost state`() {
        val store = store(budgetMs = 60_000)
        val c = setup(store)
        val a: CompletableFuture<CommitResult>
        val cc: CompletableFuture<CommitResult>
        pause()
        try {
            a = async { store.commit(c, append(c, "a.parquet"), null) }
            awaitSent("A's write", p = writeOf("a.parquet"))
            Thread.sleep(300)
            cc = async { store.commit(c, append(c, "c.parquet"), null) }
            awaitSent("C's interior plan read") { it.interior && it.status == 200 }
            awaitSent("C's write", p = writeOf("c.parquet"))
            Thread.sleep(300)
            assertThat(a).isNotDone()
            assertThat(cc).isNotDone()
            process!!.destroyForcibly().waitFor()
            // the dead owner acknowledged neither
            Thread.sleep(300)
            assertThat(seen.filter { it.isWriteTx && it.status == 200 }).isEmpty()
        } finally {
            unpause()
        }
        startEngine()
        val ra = a.get(60, TimeUnit.SECONDS)
        val rc = cc.get(60, TimeUnit.SECONDS)
        // each commit landed exactly once, at the snapshot it was acked with
        val fs = files(store, c)
        assertThat(fs).containsExactlyInAnyOrder("s3://$BUCKET/$c/data/a.parquet", "s3://$BUCKET/$c/data/c.parquet")
        val snaps = store.listSnapshots(c, 0, 1000, null).first.map { it.snapshotId }
        assertThat(snaps).contains(ra.snapshotId, rc.snapshotId)
        assertThat(ra.snapshotId).isNotEqualTo(rc.snapshotId)
        seen.filter { it.path.endsWith("/tx") }.forEach {
            println("fail-stop tx: ${it.txid} interior=${it.interior} token=${it.incarnation} status=${it.status}")
        }
        // per txid: an attempt the client timed out on while the new owner
        // was still acquiring the tenant is answered (412) too, unread
        val fenced = seen.filter { it.isWriteTx && it.status == 412 }.map { it.txid }.toSet()
        // if C's queued write was lost with its owner, its retry met the new
        // incarnation and was decided again on a parked plan; if the dead
        // owner's PUTs landed, the retry found C's receipt
        assertThat(store.interiorRedecided.get()).isEqualTo(fenced.size.toLong())
        println(
            "fail-stop path: ${fenced.size} of 2 commits fenced and redecided; the rest replayed from landed segments",
        )
    }

    private fun s3(): S3Client =
        S3Client.builder()
            .endpointOverride(URI.create(s3Endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.userName, minio.password)),
            )
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build()

    private fun findOnPath(name: String): String? =
        System.getenv("PATH")?.split(':')?.map { "$it/$name" }?.firstOrNull { File(it).canExecute() }

    companion object {
        const val BUCKET = "hoglite-interior-it"
    }
}
