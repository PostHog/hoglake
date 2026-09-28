package com.posthog.hoglake.api

import com.posthog.hoglake.RequestAdmission
import com.posthog.hoglake.observability.Metrics
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.request.path
import io.ktor.util.pipeline.PipelinePhase
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * The paths that must NEVER be dispatched onto the blocking pool.
 *
 * `/healthz` is the whole point of #218: it has to answer while every
 * request thread is stuck, so it cannot be behind those threads in a
 * queue. It does its own work on its own connection and its own
 * dispatcher ([com.posthog.hoglake.HealthProbe]), so leaving it on the
 * Netty call thread costs that thread nothing but the handoff.
 *
 * `/livez` does no IO at all, and `/metrics` reads an in-memory
 * registry — both are the surfaces an operator reaches for precisely
 * when the request path is wedged, which is the moment the queue they
 * would otherwise sit in is longest. `/metrics` sharing the bypass is
 * also why `HOGLAKE_NETTY_CALL_GROUP_SIZE` has a floor: a scrape is CPU
 * work on the call thread, and with one call thread a scrape and a
 * probe would serialize.
 *
 * Matched against [normalisedPath]; see there for what that is worth
 * today and what it is worth if anything ever changes.
 */
val PROBE_PATHS: Set<String> = setOf("/healthz", "/livez", "/metrics")

/**
 * A request path reduced to the form [PROBE_PATHS] is written in: no
 * trailing slash, and `/` for the root.
 *
 * As this server is configured TODAY, `/healthz/` matches no route at
 * all — Ktor 3's `IgnoreTrailingSlash` routing plugin is not installed,
 * so the slash spelling is a 404 (measured in
 * `RequestDispatchIntegrationTest`, not assumed; a kubelet probe must
 * be configured without it). Normalising is worth doing anyway, for
 * two reasons that do not depend on that staying true:
 *
 *  - it makes the 404 FAST. Without it the slash spelling is dispatched
 *    into the blocking queue, so under saturation the caller gets a
 *    timeout rather than an answer — the difference between a wrong
 *    answer and no answer.
 *  - it removes a silent trap. Installing `IgnoreTrailingSlash` is a
 *    one-line, entirely reasonable change that would turn `/healthz/`
 *    into a REAL probe — routed, on an exact-match bypass, straight
 *    through the saturated pool this whole file exists to keep it out
 *    of. Nothing would red.
 */
internal fun normalisedPath(call: ApplicationCall): String = call.request.path().trimEnd('/').ifEmpty { "/" }

/**
 * Take blocking work off the Netty event loop (#218), at ONE seam.
 *
 * ## The defect
 *
 * `Main.kt` starts `embeddedServer(Netty, ...)`, route handlers call
 * blocking JDBI/JDBC directly inside the coroutine, and Ktor sizes
 * Netty's call group from `availableProcessors` — which on a one-CPU
 * production pod is a single thread. Every request in the incident log
 * ran on `eventLoopGroupProxy-4-1`. A `commit/prepared` that waits the
 * full 30 s admission bound for the per-catalog advisory lock owns that
 * thread for 30 s; the kubelet's three 5 s `/healthz` probes all land
 * inside the wait and none of them is served, liveness kills the pod,
 * and every in-flight and queued commit is discarded so the writers
 * retry at once. Measured on gigahog-prod-us 2026-09-24/25: restart
 * counts 5 -> 13 per pod in two hours, `503 ... commit/prepared in
 * 30069ms` at 05:34:49.415 and the kill at 05:34:50. `/healthz` itself
 * answers in 2 ms whenever the thread is free.
 *
 * ## The mechanism, and why this one
 *
 * One interceptor at [ApplicationCallPipeline.ApplicationPhase.Plugins]
 * runs the REST of the pipeline — routing, the handler, serialization —
 * under a bounded dispatcher. Installed once by `App.module`, so a new
 * route cannot forget it and a handler needs no `withContext` of its
 * own; `Plugins` is ahead of `Call`, where Routing runs, so it covers
 * every route including ones registered after it.
 *
 * Rejected alternatives:
 *
 *  - **`withContext(Dispatchers.IO)` per handler.** Same effect,
 *    applied ~90 times, and load-bearing only where somebody remembered
 *    it. The failure mode is a new route that looks identical to its
 *    neighbours and reintroduces the incident; nothing reds.
 *  - **`Dispatchers.IO.limitedParallelism(n)`.** The right semantics
 *    (bounded view of a shared elastic pool, excess tasks queue) but it
 *    exposes neither queue depth nor active count, and #218 asks for a
 *    saturation gauge. A dedicated [ThreadPoolExecutor] gives both off
 *    `getActiveCount`/`getQueue().size` with no bookkeeping of our own,
 *    and its threads carry a name (`hoglake-request-N`) that makes a
 *    thread dump legible — the incident was diagnosed off a thread
 *    NAME in a log line.
 *  - **Netty `callGroupSize` alone.** It does fix this on Netty (see
 *    `Config.nettyCallGroupSize`, which raises it anyway as defence in
 *    depth) but it is engine configuration: the Ktor test engine has no
 *    call group, so the property could not be tested where most of this
 *    suite runs, and a pool sized for blocking work is not a pool you
 *    want doing Netty's IO. It is kept as a floor, not as the fix.
 *
 * ## What still runs off this dispatcher, and why it is fine
 *
 * The seam is not a claim that no byte of work touches a Netty thread.
 * Three things deliberately do, and each is bounded:
 *
 *  - **StatusPages' handlers** (`ErrorMapping.kt`) run on the call
 *    thread, because `StatusPages` sits at the `Monitoring` phase,
 *    ahead of this interceptor. What they do is build a small DTO,
 *    `call.respond` it, and — on the 500 branch — hand a line to
 *    logback. The EXPENSIVE part of an error, the database work that
 *    produced the exception, already happened inside this dispatcher.
 *  - **The bypassed paths** ([PROBE_PATHS]) run on the call thread on
 *    purpose. `/healthz` immediately suspends onto the probe's own
 *    dispatcher; `/livez` is a string; `/metrics` is a registry scrape.
 *  - **Ktor's Jackson deserialization** runs on `Dispatchers.IO`, not
 *    here: `ContentNegotiation`'s converter dispatches the parse
 *    itself. So a handler suspended in `call.receive()` holds no thread
 *    of this pool, [RequestDispatcher.active] UNDERCOUNTS the requests
 *    actually in flight, and a body-heavy route genuinely does get some
 *    concurrency from threads the database pool could not back. That is
 *    why `HOGLAKE_REQUEST_THREADS` is a knob at all rather than a
 *    constant — but it is still refused above `HOGLAKE_DB_POOL_SIZE` at
 *    boot, because the concurrency it buys is a fraction of a request
 *    and the Hikari timeouts it buys are whole 500s. Raise the pool
 *    with it.
 *
 * ## Under saturation
 *
 * The executor is fixed-width with an UNBOUNDED [LinkedBlockingQueue],
 * so when all threads are in a blocking call the next request queues
 * rather than erroring. Depth is not what bounds it — AGE is.
 * [installBlockingDispatch] stamps `System.nanoTime()` before the
 * hand-off, records the wait as `hoglake_request_queue_wait_seconds` at
 * handler entry, and:
 *
 *  - **sheds** a request whose queue wait already exhausted the commit
 *    admission bound, with the same typed 503 + Retry-After a queued
 *    commit gets, BEFORE it borrows a pooled connection
 *    ([RequestAdmission.refuseIfQueueExhausted]); and
 *  - **charges** the remainder of the wait against the advisory-lock
 *    bound inside `Locks.acquireCatalogCommitLock`
 *    ([RequestAdmission.remainingLockTimeoutMs]), so a request cannot
 *    spend the admission budget twice.
 *
 * A bound on DEPTH was rejected: the number that matters to a caller is
 * how long it waited, a depth cap turns a brief burst into refusals
 * while a slow wedge under the cap stays invisible, and the cap itself
 * would be a second knob nobody could size.
 *
 * What must remain true under saturation is that the PROBE is still
 * served, and that is [PROBE_PATHS]. Note what this does NOT do:
 * `/healthz` answers 200 while the request path is wedged, and
 * `/healthz` is wired to readiness, so a saturated pod stays in the
 * Service. That is deliberate for now — shedding by age is the
 * backpressure, and readiness-on-sustained-saturation is a separate
 * change with its own chart coupling (#218 item 2).
 *
 * Width defaults to `HOGLAKE_DB_POOL_SIZE`; the argument for that
 * number, and what it costs to raise it, is on `Config.requestThreads`.
 */
fun Application.installBlockingDispatch(
    requests: RequestDispatcher,
    admissionBudgetMs: Long,
    bypass: Set<String> = PROBE_PATHS,
) {
    insertPhaseAfter(ApplicationCallPipeline.ApplicationPhase.Plugins, BlockingDispatchPhase)
    intercept(BlockingDispatchPhase) {
        if (normalisedPath(call) in bypass) {
            proceed()
            return@intercept
        }
        val admission = RequestAdmission(System.nanoTime())
        requests.enqueued()
        var admitted = false
        try {
            withContext(requests.dispatcher + RequestAdmission.element(admission)) {
                admitted = true
                requests.admitted()
                admission.admit(System.nanoTime())
                Metrics.requestQueueWait(admission.queueWaitMs())
                if (RequestAdmission.queueExhausted(admissionBudgetMs, admission.queueWaitMs())) {
                    Metrics.requestShed()
                    RequestAdmission.refuseIfQueueExhausted(admissionBudgetMs, admission.queueWaitMs())
                }
                proceed()
            }
        } finally {
            // The block never ran (the call was cancelled before it
            // reached a thread): the waiting count is still ours, and
            // the wait it spent is invisible to the histogram, which
            // only records calls that survived to be admitted.
            if (!admitted) {
                requests.admitted()
                Metrics.requestQueueAbandoned()
            }
        }
    }
}

/**
 * The seam's own pipeline phase, BETWEEN `Plugins` and `Call`.
 *
 * Not `Plugins` itself, which is where it started. Interceptors in one
 * phase run in registration order, and this is installed first, so
 * everything else at `Plugins` — `RequestId` above all — ran only via
 * `proceed()`, i.e. INSIDE the dispatcher. A request shed here
 * therefore threw before `RequestId` had minted one, and the 503 went
 * out with no `X-Request-Id` for an operator to correlate: the one
 * response class most likely to be investigated was the one that could
 * not be.
 *
 * After `Plugins` and before `Call` is the whole requirement. Routing
 * lives at `Call`, so every route still runs inside the dispatcher;
 * `RequestId`, `CallLogging`'s setup and `ContentNegotiation`'s
 * registration run ahead of it on the Netty call thread, which costs a
 * UUID and two map writes and is bounded by nothing a request can do.
 * `StatusPages` sits further out still, at `Monitoring`, so it catches
 * the shed either way.
 */
internal val BlockingDispatchPhase = PipelinePhase("HoglakeBlockingDispatch")

/**
 * The bounded pool [installBlockingDispatch] runs handlers on, plus the
 * numbers that say how close it is to full.
 *
 * Fixed width with an unbounded FIFO queue, and `allowCoreThreadTimeOut`
 * so an idle instance (every `testApplication` in the suite builds one)
 * holds no threads. Threads are daemons: a wedged handler must not keep
 * a shutting-down JVM alive, since the reason it is shutting down is
 * usually that same wedge.
 */
class RequestDispatcher(
    val threads: Int,
    threadNamePrefix: String = "hoglake-request",
) : AutoCloseable {
    private val threadCounter = AtomicLong()
    private val waiting = AtomicInteger()

    private val executor: ThreadPoolExecutor =
        ThreadPoolExecutor(
            threads,
            threads,
            KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            LinkedBlockingQueue(),
            { runnable ->
                Thread(runnable, "$threadNamePrefix-${threadCounter.incrementAndGet()}")
                    .apply { isDaemon = true }
            },
        ).apply { allowCoreThreadTimeOut(true) }

    val dispatcher: CoroutineDispatcher = executor.asCoroutineDispatcher()

    /** A call has been handed to the dispatcher and has not started yet. */
    internal fun enqueued() {
        waiting.incrementAndGet()
    }

    /** That call reached a thread (or was abandoned before it could). */
    internal fun admitted() {
        waiting.decrementAndGet()
    }

    /**
     * Threads currently RUNNING a task — a handler inside a blocking
     * call.
     *
     * `ThreadPoolExecutor.getActiveCount` is documented as an
     * APPROXIMATION (it walks the worker set under the pool lock and
     * reports what it saw), and it counts THREADS rather than requests:
     * a handler suspended in `call.receive()` is parsing its body on
     * `Dispatchers.IO` and is not counted here. Both readings are the
     * right ones for the question this gauge answers — how much of the
     * pool is unavailable right now, which is the quantity that starved
     * the probe — but neither makes it an exact request count.
     */
    val active: Int get() = executor.activeCount

    /**
     * Calls waiting for their FIRST thread.
     *
     * Deliberately not `executor.queue.size`: the executor's queue
     * holds every dispatch, and a coroutine already admitted posts a
     * fresh task to it on each resumption (after `call.receive()`,
     * after a suspending write). Queue depth therefore counts admitted
     * work as if it were waiting, which is the opposite of what a
     * saturation gauge should say. This counter is incremented in the
     * interceptor before the hand-off and decremented the moment the
     * call reaches a thread, so it counts exactly the requests that
     * have not started.
     */
    val queued: Int get() = waiting.get()

    /**
     * Stop accepting and let what is running finish.
     *
     * `shutdown()` and a bounded wait, never `shutdownNow()`:
     * interrupting an in-flight commit is precisely the behaviour #218
     * complains about — the caller loses a commit it had every reason
     * to think was proceeding. The wait is bounded because a wedged
     * handler must not hold a shutdown open; the threads are daemons,
     * so what is still running when the wait expires dies with the JVM.
     */
    override fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
            executor.shutdownNow()
        }
    }

    private companion object {
        const val KEEP_ALIVE_SECONDS = 60L
        const val SHUTDOWN_WAIT_SECONDS = 5L
    }
}
