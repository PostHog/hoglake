package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * ONE BUDGET, CHARGED ONCE (#218).
 *
 * Moving blocking work off the event loop turns starvation into
 * queueing, and a queue nobody accounts for is a second invisible
 * budget. `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` promises callers ONE bounded
 * wait before a typed 503 + Retry-After; without the accounting here a
 * request would serve that bound twice — once waiting for a handler
 * thread and once waiting for the advisory lock — and a caller long
 * gone would still be costing a connection and a lock slot.
 *
 * Both halves need a REAL engine on a real port: the Ktor test engine
 * has no call group and dispatches calls on the test's own coroutine
 * dispatcher, so neither the queue nor the wait that has to be charged
 * against the bound exists there.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RequestSheddingIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    /**
     * ONE handler thread, so a single blocked request puts everything
     * behind it, and a short admission bound so the queue wait passes it
     * inside a test's patience. The pool stays at its default width:
     * the assertion is that a shed request never REACHES it.
     */
    private val cfg =
        Config(
            hydratorIntervalMs = 0,
            metricsIntervalMs = 0,
            maintenanceSummaryIntervalMs = 0,
            jdbcUrl = db.jdbcUrl,
            dbUser = PgTestSupport.USER,
            dbPassword = PgTestSupport.PASSWORD,
            requestThreads = 1,
            commitLockTimeoutMs = ADMISSION_MS,
        )
    private val ds = Database.dataSource(cfg)
    private val app = App.build(cfg, Database.jdbi(ds), dataSource = ds)
    private val catalogs = CatalogService(db.jdbi)
    private val json = ObjectMapper()

    @Volatile
    private var gate = CountDownLatch(1)
    private val inHandler = AtomicInteger()

    /** Which catalog `/test/slow-lock` should queue on. */
    private val lockVictimCatalogId = java.util.concurrent.atomic.AtomicLong()

    private lateinit var server: EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>
    private var port = 0
    private val http: HttpClient =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build()
    private val clients = Executors.newCachedThreadPool()

    @BeforeAll
    fun boot() {
        server =
            embeddedServer(
                Netty,
                configure = {
                    connector { port = 0 }
                    callGroupSize = cfg.nettyCallGroupSize
                },
            ) {
                app.module(this)
                routing {
                    get("/test/block") {
                        inHandler.incrementAndGet()
                        try {
                            gate.await(30, TimeUnit.SECONDS)
                        } finally {
                            inHandler.decrementAndGet()
                        }
                        call.respondText("unblocked")
                    }
                    // Sleeps past the admission bound, THEN queues on a
                    // catalog lock the test holds, and reports how long
                    // the lock wait took. It goes through the same seam
                    // as every real route, so it carries the same
                    // ambient admission a commit handler would.
                    get("/test/slow-lock") {
                        Thread.sleep(SLOW_HANDLER_MS)
                        val started = System.nanoTime()
                        runCatching {
                            db.jdbi.useTransaction<Exception> { h ->
                                Locks.acquireCatalogCommitLock(h, lockVictimCatalogId.get(), cfg.commitLockTimeoutMs)
                            }
                        }
                        call.respondText("${(System.nanoTime() - started) / 1_000_000}")
                    }
                }
            }
        server.start(wait = false)
        port = runBlocking { server.engine.resolvedConnectors().first().port }
    }

    @AfterAll
    fun shutdown() {
        gate.countDown()
        clients.shutdownNow()
        server.stop(0, 1, TimeUnit.SECONDS)
        app.close()
        ds.close()
        db.close()
    }

    // ---- helpers ---------------------------------------------------------

    private fun request(
        method: String,
        path: String,
        body: String? = null,
    ): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path")).timeout(Duration.ofSeconds(60))
        if (body == null) {
            builder.GET()
        } else {
            builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body))
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    /** Hold the one handler thread until [release] is called. */
    private fun holdTheOnlyHandlerThread() {
        gate.countDown()
        gate = CountDownLatch(1)
        inHandler.set(0)
        clients.submit { runCatching { request("GET", "/test/block") } }
        await().atMost(Duration.ofSeconds(10)).until { inHandler.get() == 1 }
    }

    private fun release() {
        gate.countDown()
        await().atMost(Duration.ofSeconds(10)).until { inHandler.get() == 0 }
    }

    private fun seed(catalog: String): Long {
        catalogs.createCatalog(catalog, "s3://shed/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        return db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, catalog).mapTo(Long::class.java).one()
        }
    }

    // ---- the two halves --------------------------------------------------

    /**
     * A request whose QUEUE WAIT already exhausted the admission bound
     * is shed with the typed 503 + Retry-After, and never borrows a
     * pooled connection to find out.
     *
     * `/v1/catalogs` is the subject deliberately: it is an ordinary read
     * that WOULD take a connection, so a 200 here would mean the handler
     * ran, and `hoglake_db_pool_active` staying at zero is what says it
     * did not. Shedding is not commit-specific — a GET that waited past
     * the bound is a GET whose caller has gone, and serving it spends a
     * thread the requests still waiting need.
     *
     * MUTATION: delete the `refuseIfQueueExhausted` call from
     * `installBlockingDispatch` and this reds with a 200 — the request
     * is served, several seconds late, on behalf of nobody.
     *
     * MUTATION: answer a bare 503 instead of throwing the typed
     * `CommitQueueTimeout` and the `commit_queue_timeout` body and the
     * Retry-After header both red; clients that already know how to back
     * off would stop recognising the signal.
     *
     * NOTE WHAT THE FIXTURE HAS TO DO, because it is the honest shape of
     * the mechanism: the thread is freed before the assertions. The
     * queue wait is judged at ADMISSION — when a call reaches a thread —
     * not on arrival, so a pool that never frees a thread queues rather
     * than refuses. That is the right trade for a transient convoy (the
     * queue drains and the stale head of it is discarded instead of
     * served) and it is exactly why readiness-on-sustained-saturation
     * is still needed for the other case (#218 item 2): today a wedged
     * pod answers /healthz 200 and stays in the Service.
     */
    @Test
    fun `a request that queued past the admission bound is shed before it takes a connection`() {
        holdTheOnlyHandlerThread()
        val catalogsBefore = catalogs.listCatalogs().size
        val shedBefore = shedCount()
        val queued =
            clients.submit<Pair<HttpResponse<String>, Long>> {
                val started = System.nanoTime()
                val response =
                    request("POST", "/v1/catalogs", """{"name":"shed-never-created","data_path":"s3://shed/never"}""")
                response to (System.nanoTime() - started) / 1_000_000
            }
        // Keep it queued past the bound, THEN free the thread. The check
        // happens at admission, not on arrival (see below), so the queue
        // has to drain for the shed to be reached.
        Thread.sleep(ADMISSION_MS + 500)
        release()

        val (shed, elapsedMs) = queued.get(60, TimeUnit.SECONDS)
        assertThat(shed.statusCode()).isEqualTo(503)
        assertThat(json.readTree(shed.body())["error"].asText()).isEqualTo("commit_queue_timeout")
        assertThat(shed.headers().firstValue("Retry-After")).isPresent()
        // The id is what an operator correlates the 503 with, and the
        // seam used to shed AHEAD of the RequestId plugin, so the one
        // response class most likely to be investigated was the one
        // that carried no id. MUTATION: intercept at
        // `ApplicationCallPipeline.ApplicationPhase.Plugins` again
        // instead of at BlockingDispatchPhase and this reds.
        assertThat(shed.headers().firstValue("X-Request-Id"))
            .describedAs("a shed 503 must still be correlatable")
            .isPresent()
        assertThat(elapsedMs).isGreaterThanOrEqualTo(ADMISSION_MS)

        // THE HANDLER NEVER RAN, asserted on durable state rather than
        // on a point-in-time pool gauge — `hoglake_db_pool_active` is
        // zero whether or not a connection was borrowed and returned,
        // so reading it once proves nothing. A POST that reached the
        // handler would have created a catalog; a shed one cannot.
        assertThat(catalogs.listCatalogs().size)
            .describedAs("a shed request must not have reached the handler, let alone a connection")
            .isEqualTo(catalogsBefore)
        assertThat(catalogs.listCatalogs().map { it.name }).doesNotContain("shed-never-created")

        // And it is counted where a shed is counted: not in the
        // per-catalog commit counter (no catalog was parsed), so the
        // refusals would otherwise be invisible.
        assertThat(shedCount()).isEqualTo(shedBefore + 1.0)
        assertThat(app.meterRegistry.get("hoglake_request_queue_wait").timer().count()).isGreaterThan(0)
        assertThat(app.meterRegistry.scrape()).contains("hoglake_request_queue_wait_seconds_count")
    }

    private fun shedCount(): Double = app.meterRegistry.find("hoglake_requests_shed_total").counter()?.count() ?: 0.0

    /**
     * A handler that RUNS for longer than the admission bound and then
     * takes the commit lock still gets the whole remaining bound.
     *
     * This is the difference between queue wait and elapsed-since-
     * dispatch, at the HTTP level. `POST .../maintenance/compact` runs
     * a synchronous sweep on the handler thread, so with the wait read
     * live every group after the first `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS`
     * of the run would take the lock with the 1 s floor and 503 blaming
     * a queue wait that never happened.
     *
     * MUTATION: define `RequestAdmission.queueWaitMs()` as
     * `(System.nanoTime() - enqueuedNanos) / 1_000_000` and this reds —
     * the lock wait collapses to the floor.
     */
    @Test
    fun `a handler that runs past the admission bound still gets the whole lock bound`() {
        val catalogId = seed("shed-slow-handler")
        val lockHeld = CountDownLatch(1)
        val releaseLock = CountDownLatch(1)
        val holder =
            thread(name = "shed-slow-lock-holder") {
                db.jdbi.useTransaction<Exception> { h ->
                    Locks.acquireCatalogCommitLock(h, catalogId)
                    lockHeld.countDown()
                    releaseLock.await(60, TimeUnit.SECONDS)
                }
            }
        try {
            assertThat(lockHeld.await(10, TimeUnit.SECONDS)).isTrue()
            lockVictimCatalogId.set(catalogId)

            // No queue at all: the handler is admitted immediately and
            // then spends longer than the bound doing its own work.
            val waitedMs = request("GET", "/test/slow-lock").body().toLong()

            assertThat(waitedMs)
                .describedAs("the handler's own execution must not be charged against the lock bound")
                .isGreaterThanOrEqualTo(ADMISSION_MS - 500)
        } finally {
            releaseLock.countDown()
            holder.join(TimeUnit.SECONDS.toMillis(30))
        }
    }

    /**
     * A commit that queued for a handler thread reaches the advisory
     * lock with the REMAINDER of the admission bound, not a fresh copy
     * of it.
     *
     * The fixture holds the catalog commit lock elsewhere, so the commit
     * is guaranteed to wait the whole remaining bound and 503. With the
     * queue wait charged, its total is about one admission bound; with a
     * fresh copy it is two, which is the "55 s of server time" case at
     * production settings.
     *
     * MUTATION: return `configured` unchanged from
     * `RequestAdmission.remainingLockTimeoutMs` (the subtraction
     * dropped) and the upper bound reds at roughly double.
     */
    @Test
    fun `a commit charges its queue wait against the lock bound`() {
        val catalogId = seed("shed-charge")
        val lockHeld = CountDownLatch(1)
        val releaseLock = CountDownLatch(1)
        val holder =
            thread(name = "shed-lock-holder") {
                db.jdbi.useTransaction<Exception> { h ->
                    Locks.acquireCatalogCommitLock(h, catalogId)
                    lockHeld.countDown()
                    releaseLock.await(60, TimeUnit.SECONDS)
                }
            }
        try {
            assertThat(lockHeld.await(10, TimeUnit.SECONDS)).isTrue()

            // Queue the commit behind a handler held for most of the
            // admission bound, so it arrives at the lock with a small
            // remainder rather than the whole thing.
            holdTheOnlyHandlerThread()
            val commit =
                clients.submit<Pair<HttpResponse<String>, Long>> {
                    val started = System.nanoTime()
                    val response =
                        request(
                            "POST",
                            "/v1/catalogs/shed-charge/commit",
                            """
                            {"appends":[{"namespace":"ns","table":"t","files":[
                                {"path":"s3://shed/shed-charge/f1.parquet","record_count":1,"file_size_bytes":10}
                            ]}]}
                            """.trimIndent(),
                        )
                    response to (System.nanoTime() - started) / 1_000_000
                }
            // Hold the thread for most of the bound, then let the commit
            // through to the lock it cannot have.
            Thread.sleep(QUEUE_HOLD_MS)
            release()

            val (response, elapsedMs) = commit.get(60, TimeUnit.SECONDS)
            assertThat(response.statusCode()).isEqualTo(503)
            assertThat(json.readTree(response.body())["error"].asText()).isEqualTo("commit_queue_timeout")
            // Queue wait plus the REMAINING bound, not queue wait plus a
            // whole fresh bound.
            assertThat(elapsedMs)
                .describedAs("the queue wait must be charged against the lock bound, not doubled")
                .isLessThan(ADMISSION_MS + QUEUE_HOLD_MS / 2)
        } finally {
            releaseLock.countDown()
            holder.join(TimeUnit.SECONDS.toMillis(30))
        }
    }

    private companion object {
        /**
         * The admission bound for this fixture. Well above
         * `RequestAdmission.MIN_REMAINING_LOCK_TIMEOUT_MS` so the
         * subtraction has room to be visible, and small enough that
         * waiting it out twice is still a fast test.
         */
        const val ADMISSION_MS = 4_000L

        /**
         * How long `/test/slow-lock` spends in the handler before it
         * reaches for the lock — past [ADMISSION_MS], so a wait read
         * live rather than frozen at admission would already look
         * exhausted by the time the handler takes the lock.
         */
        const val SLOW_HANDLER_MS = 5_000L

        /**
         * How long the commit is held in the queue before it may run:
         * under [ADMISSION_MS], so it is NOT shed and goes on to the
         * lock — with a remainder of one second rather than a fresh
         * four. The gap between "charged" (~4 s total) and "doubled"
         * (~7 s) is what the assertion discriminates on.
         */
        const val QUEUE_HOLD_MS = 3_000L
    }
}
