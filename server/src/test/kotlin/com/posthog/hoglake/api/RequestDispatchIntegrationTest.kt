package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
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
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * #218, against the REAL Netty engine on a real port.
 *
 * This suite reaches the API through `testApplication` almost
 * everywhere, and the test engine cannot see the defect: it has no
 * Netty call group and no event loop — it runs each call on the test's
 * own coroutine dispatcher — so a handler that blocks there blocks
 * nothing a probe needs, and `/healthz` would answer 200 whether or not
 * the fix existed. The property only means something where production
 * runs it, so this class boots `embeddedServer(Netty, ...)` with the
 * same `callGroupSize` configuration `Main.kt` uses and speaks to it
 * over TCP with the JDK's own HTTP client.
 *
 * `/test/block` stands in for a handler inside a long blocking call —
 * in production, a `commit/prepared` queued on the per-catalog advisory
 * lock for the full 30 s admission bound. A latch rather than a real
 * lock wait because what is under test is the DISPATCH, not the lock:
 * the test has to hold exactly `requestThreads` handlers and release
 * them on command, and a real convoy holds an unpredictable number for
 * an unpredictable time.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RequestDispatchIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    /** Small enough to saturate quickly; nothing here depends on the number. */
    private val requestThreads = 4

    private val cfg =
        Config(
            hydratorIntervalMs = 0,
            metricsIntervalMs = 0,
            maintenanceSummaryIntervalMs = 0,
            jdbcUrl = db.jdbcUrl,
            dbUser = PgTestSupport.USER,
            dbPassword = PgTestSupport.PASSWORD,
            requestThreads = requestThreads,
        )
    private val app = App.build(cfg, db.jdbi, dataSource = db.dataSource)

    /** Released by each test; handlers wait on it instead of on a lock. */
    @Volatile
    private var gate = CountDownLatch(1)
    private val inHandler = AtomicInteger()

    private lateinit var server: EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>
    private var port = 0

    /**
     * HTTP/1.1, pinned. The JDK client negotiates HTTP/2 by default, and
     * over h2 a single connection multiplexes every one of these
     * requests: the concurrency this test constructs would then depend
     * on Netty's `runningLimit` and on stream scheduling rather than on
     * the dispatcher width it means to measure.
     */
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
                    // Main.kt's configuration, from the same knob. The
                    // WORKER group keeps Ktor's own default there too.
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
                    get("/test/thread") {
                        call.respondText(Thread.currentThread().name)
                    }
                    get("/test/sleep") {
                        Thread.sleep(SLEEP_MS)
                        call.respondText("slept")
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
        db.close()
    }

    // ---- helpers ---------------------------------------------------------

    private fun getSync(
        path: String,
        timeout: Duration = Duration.ofSeconds(30),
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path")).timeout(timeout).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun fire(path: String): Future<HttpResponse<String>> =
        clients.submit<HttpResponse<String>> {
            getSync(
                path,
            )
        }

    /** A fresh latch for the next test; whatever is still waiting goes free. */
    private fun resetGate() {
        gate.countDown()
        gate = CountDownLatch(1)
        inHandler.set(0)
    }

    /**
     * Release everything and install NOTHING new. Resetting at the end
     * instead would re-arm the latch under the requests still sitting
     * in the dispatcher QUEUE: they enter the handler only once a thread
     * frees, read the field then, and would block on the new latch for
     * its whole 30 s.
     */
    private fun releaseAll() {
        gate.countDown()
    }

    private fun gaugeValue(name: String): Double = app.meterRegistry.get(name).gauge().value()

    // ---- the fix -----------------------------------------------------

    /**
     * (i) Every request thread blocked, and `/healthz` still answers 200
     * inside a second.
     *
     * MUTATION: delete the `installBlockingDispatch` call from
     * `App.module` (handlers back on the Netty call group), or drop
     * `/healthz` from `PROBE_PATHS` so the probe is dispatched onto the
     * saturated pool. Either reds here — the second with the request
     * timing out at one second, the first with it timing out once the
     * call group is full too.
     *
     * (iv) rides along, because the gauges have to be readable in
     * exactly this state to be worth anything: `active` pinned at the
     * width, `queued` at the excess, `max` at the configured width.
     */
    @Test
    fun `healthz answers while every request thread is blocked`() {
        resetGate()
        val queuedExtra = 3
        val blocked = (1..requestThreads).map { fire("/test/block") }
        await().atMost(Duration.ofSeconds(10)).until { inHandler.get() == requestThreads }

        // Pile more on: these cannot start, so they sit in the
        // dispatcher's queue. A probe behind THEM would never answer.
        val queued = (1..queuedExtra).map { fire("/test/block") }
        await().atMost(Duration.ofSeconds(10)).until { app.requestDispatcher.queued == queuedExtra }

        val started = System.nanoTime()
        val health = getSync("/healthz", timeout = Duration.ofSeconds(1))
        val elapsedMs = (System.nanoTime() - started) / 1_000_000

        assertThat(health.statusCode()).isEqualTo(200)
        assertThat(health.body()).isEqualTo("ok")
        assertThat(elapsedMs).isLessThan(1_000)

        // (iv) — the saturation the probe just saw through, as numbers.
        // `queued` is exact (the interceptor counts first dispatches);
        // `active` is ThreadPoolExecutor.getActiveCount, documented as
        // an approximation, so it is asserted as the range the fixture
        // pins: every thread is inside gate.await() and none can be
        // anywhere else.
        assertThat(gaugeValue("hoglake_request_pool_queued")).isEqualTo(queuedExtra.toDouble())
        assertThat(gaugeValue("hoglake_request_pool_active"))
            .isBetween(requestThreads.toDouble() - 1, requestThreads.toDouble())
        assertThat(gaugeValue("hoglake_request_pool_max")).isEqualTo(requestThreads.toDouble())

        // /livez and /metrics are on the same bypass and must answer too.
        assertThat(getSync("/livez", timeout = Duration.ofSeconds(1)).statusCode()).isEqualTo(200)
        val scrape = getSync("/metrics", timeout = Duration.ofSeconds(2))
        assertThat(scrape.statusCode()).isEqualTo(200)
        assertThat(scrape.body()).contains("hoglake_request_pool_active")

        releaseAll()
        (blocked + queued).forEach { assertThat(it.get(30, TimeUnit.SECONDS).statusCode()).isEqualTo(200) }
    }

    /**
     * The bypass is matched on a NORMALISED path, so a probe spelling
     * that differs only by a trailing slash cannot be dispatched into
     * the queue the bypass exists to skip.
     *
     * Two facts, and the test pins both because the second is only
     * meaningful given the first. **`/healthz/` is a 404 here**: this
     * server does not install Ktor 3's `IgnoreTrailingSlash` routing
     * plugin, so the slash spelling matches no route — measured, not
     * assumed, and the reason a kubelet probe must be configured
     * without it. **And it is a FAST 404**, because normalisation keeps
     * it out of the dispatcher: without it the request queues behind
     * every blocked handler and the caller gets a timeout instead of an
     * answer. The normalisation is therefore defence against a future
     * `IgnoreTrailingSlash` (which would make this spelling a real
     * probe, silently routed through the saturated pool) and, today,
     * the difference between a wrong answer and no answer.
     *
     * MUTATION: match on `call.request.path()` instead of
     * [normalisedPath] and this reds — the one-second client timeout
     * expires with the request still in the queue.
     */
    @Test
    fun `the probe bypass survives a trailing slash`() {
        resetGate()
        val blocked = (1..requestThreads).map { fire("/test/block") }
        await().atMost(Duration.ofSeconds(10)).until { inHandler.get() == requestThreads }
        try {
            val started = System.nanoTime()
            val health = getSync("/healthz/", timeout = Duration.ofSeconds(1))
            val elapsedMs = (System.nanoTime() - started) / 1_000_000
            assertThat(health.statusCode())
                .describedAs("IgnoreTrailingSlash is not installed, so this spelling matches no route")
                .isEqualTo(404)
            assertThat(elapsedMs)
                .describedAs("but it never entered the blocking queue, which is what normalisation buys")
                .isLessThan(1_000)
        } finally {
            releaseAll()
            blocked.forEach { it.get(30, TimeUnit.SECONDS) }
        }
    }

    /**
     * Handlers run on the named blocking pool, never on a Netty thread.
     *
     * The incident was diagnosed off a thread NAME — every request in
     * the log ran on `eventLoopGroupProxy-4-1` — so the name is the
     * assertion.
     *
     * MUTATION: remove `installBlockingDispatch` from `App.module` and
     * the reported thread becomes `eventLoopGroupProxy-*` again.
     */
    @Test
    fun `handlers run on the blocking request pool and not on a netty thread`() {
        // A real route first, to prove the seam is in the pipeline the
        // /v1 handlers run through and not only in front of the test
        // routes registered after it.
        assertThat(getSync("/v1/info").body()).contains("\"version\"")

        val handlerThread = getSync("/test/thread").body()
        assertThat(handlerThread).startsWith("hoglake-request-")
        assertThat(handlerThread).doesNotContain("eventLoopGroupProxy")
    }

    /**
     * (iii) N+1 concurrent requests against a pool of N: the extra one
     * QUEUES and completes — none error — and the wall clock shows it
     * waited for a thread rather than getting one of its own.
     *
     * Bounded on BOTH sides. The lower bound is the queueing itself; the
     * upper bound is what says the queue drained promptly rather than
     * serializing — with a lower bound alone, a dispatcher of ONE thread
     * (five sequential sleeps, five seconds) would pass a test that
     * claims to be about a pool of four.
     *
     * MUTATION: widen the DISPATCHER alone — `requestThreads = 4` for
     * the test's N, `Config(requestThreads = requestThreads + 1)` — and
     * the lower bound reds, because nothing queues. (Raising the field
     * raises N with it and proves nothing; that mutation passes.)
     * MUTATION: set `Config(requestThreads = 1)` and the upper bound
     * reds.
     */
    @Test
    fun `one request beyond the pool width queues and still completes`() {
        val n = requestThreads
        val peakActive = AtomicInteger()
        val peakQueued = AtomicInteger()
        val sampling =
            Thread {
                while (!Thread.currentThread().isInterrupted) {
                    peakActive.accumulateAndGet(app.requestDispatcher.active, ::maxOf)
                    peakQueued.accumulateAndGet(app.requestDispatcher.queued, ::maxOf)
                    Thread.sleep(5)
                }
            }.apply {
                isDaemon = true
                start()
            }

        val started = System.nanoTime()
        val responses = (1..n + 1).map { fire("/test/sleep") }.map { it.get(60, TimeUnit.SECONDS) }
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        sampling.interrupt()

        assertThat(responses.map { it.statusCode() }).containsOnly(200)
        assertThat(responses.map { it.body() }).containsOnly("slept")
        // N threads, N+1 tasks of SLEEP_MS each: the last one cannot
        // start until one of the first N is done, and every one of them
        // must be done inside two sleeps plus slack.
        assertThat(elapsedMs).isGreaterThanOrEqualTo(2 * SLEEP_MS - SLEEP_SLACK_MS)
        assertThat(elapsedMs).isLessThan(3 * SLEEP_MS)
        // And the gauges saw it happen: the pool filled and something
        // waited. Sampled rather than read once, because the state is
        // transient by construction.
        assertThat(peakActive.get()).isEqualTo(n)
        assertThat(peakQueued.get()).isGreaterThanOrEqualTo(1)
    }

    private companion object {
        const val SLEEP_MS = 1_000L

        /**
         * Scheduling slack on the queueing assertion. Generous on
         * purpose: the discrimination this test needs is between one
         * sleep and two, and the mutation it guards against (a wider
         * pool) lands at ~1 s, not at ~1.9 s.
         */
        const val SLEEP_SLACK_MS = 100L
    }
}
