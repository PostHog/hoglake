package com.posthog.hoglake.hoglite

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Per-backend reachability of the configured hoglite backends, sampled
 * OFF the readiness path (#678).
 *
 * ## Why hoglite is not a readiness condition
 *
 * `/healthz` used to require every configured hoglite backend to answer
 * its `/v1/healthz`, serially, two seconds each, on the probe's own
 * response path. A hoglite backend is a dependency EVERY pod shares, so
 * one unhealthy backend made every pod unready at once — and hoglited
 * answers 503 during a lease handover, so an ordinary failover marked
 * the whole fleet unready and took the PostgreSQL catalogs, which were
 * fine, out of the Service with it. Unreadiness can only ever help when
 * it is one pod's problem: with every pod unready the Service has no
 * endpoints and a caller gets a connection refused where a ready pod
 * would have given it a typed 503 `backend_unavailable` inside the
 * client's bounds.
 *
 * The zombie-server rule that keeps PostgreSQL in readiness
 * ([com.posthog.hoglake.HealthProbe]) does not transfer. That incident
 * was `/healthz` saying ok while every `/v1` call HUNG. `HogliteClient`
 * bounds a hoglite attempt's connect and response headers (a retry
 * budget, a circuit that breaks on a down pool), so a down backend
 * answers quickly and truthfully, which is what readiness asks. It does
 * not bound the body: JDK 25's `HttpClient` stops the request timer
 * once headers arrive, so a backend that sends headers and then stalls
 * blocks the call. That stall is the backend's, shared by every
 * pod, so readiness would still pull the whole fleet for it.
 *
 * ## What this class does instead
 *
 * The `hoglite_health` background loop
 * (`HOGLAKE_HOGLITE_HEALTH_INTERVAL_MS`) calls [sample] on every pod
 * with a hoglite backend configured, which starts one probe per
 * backend and returns AT ONCE; the answers land in [reachable] and are
 * published as `hoglake_hoglite_backend_reachable{backend}`, one
 * series per backend, so an operator sees WHICH backend is unhealthy
 * rather than a pod that is not ready. A loop of its own and not a
 * `/healthz` side effect, so the series is live on a pod nothing
 * probes (a Deployment without a readiness probe, a compose stack, a
 * local run) and ticks at hoglake's cadence rather than the kubelet's.
 * `/healthz` never waits for a hoglite probe, and never starts one: a
 * kubelet readiness timeout of 3 s (the chart's default) is below one
 * backend's connect-plus-headers bound, so waiting would have turned a
 * slow backend into unreadiness by timeout even once its answer
 * stopped gating the status.
 *
 * ## The bounds, which are `HealthProbe`'s
 *
 * - **Single flight per backend, with an age bound.** A tick that finds
 *   an attempt in flight and younger than its deadline starts nothing.
 *   Past the deadline the attempt is ABANDONED and a fresh one started,
 *   for the reason `HealthProbe` gives: an attempt hung below the
 *   client's own timeouts would otherwise pin the series for the rest
 *   of the pod's life.
 * - **The deadline is enforced on the series, not only on the tick.**
 *   A probe that outlasted its deadline is a failed probe, which is how
 *   `HealthProbe` reads its own, so [reachable] reads 0 from the moment
 *   the attempt in flight crosses its deadline, and that attempt
 *   publishes 0 if it answers late. The sampling interval (10 s) is
 *   longer than a default deadline (4 s): checked only on a tick, an
 *   attempt answering at 6 s would publish 1, and a backend whose
 *   probes hang would show its last completed probe's 1 until the next
 *   tick.
 * - **Only the attempt in flight publishes.** An abandoned attempt that
 *   finishes late drops its answer, so a stale false cannot overwrite
 *   the fresh attempt's true, or the reverse: the series carries the
 *   newest attempt's result.
 * - **A cap of [MAX_CONCURRENT_PROBES] outstanding attempts per
 *   backend.** With two already hung nothing more is started, so a
 *   permanent hang costs two parked threads per backend and never
 *   leaks more. The expired attempt stays the one in flight and the
 *   series reads 0; the parked attempts are
 *   `hoglake_hoglite_health_probe_attempts_hung{backend}`
 *   ([outstandingAttempts]). At the cap sampling resumes only when a
 *   parked attempt returns, so a cap that persists needs a restart.
 *   Reaching the cap and leaving it are logged once each, not on every
 *   tick of the stall.
 * - **Never throws, never blocks the caller.** A probe that throws
 *   reads as unreachable.
 *
 * The threads are a cached pool of daemons. The cap is enforced by the
 * arithmetic above, not by the pool: at most two attempts per backend
 * exist at any moment, so the pool never holds more threads than that
 * and idle ones time out.
 */
class HogliteBackendHealth(
    /** The configured backends and, for each, the age at which an attempt of its probe is abandoned. */
    deadlinesMs: Map<UUID, Long>,
    /** The probe itself: `HogliteClient.reachable()` of the backend, one bounded attempt, past the breaker. */
    private val probe: (UUID) -> Boolean,
) : AutoCloseable {
    private val log = KotlinLogging.logger {}

    /** [answered] is set, under the backend's lock, when the attempt's own answer has been taken. */
    private class Attempt(val startedNanos: Long) {
        lateinit var task: Future<*>
        var answered = false
    }

    private class Backend(val deadlineMs: Long) {
        var inFlight: Attempt? = null
        val running = AtomicInteger()
        val launched = AtomicLong()

        /** Whether the cap refused the last tick, so the refusal and the recovery are logged once each. */
        var capped = false

        /** The last answer: 1.0 reachable, 0.0 not, NaN before the first answer landed. */
        @Volatile
        var reachable: Double = Double.NaN

        /** The attempt in flight that has not answered yet, or null. Under the lock. */
        fun pending(): Attempt? = inFlight?.takeIf { !it.answered && !it.task.isDone }

        fun ageMs(
            attempt: Attempt,
            nowNanos: Long,
        ): Long = (nowNanos - attempt.startedNanos) / 1_000_000
    }

    private val backends: Map<UUID, Backend> = deadlinesMs.mapValues { Backend(it.value) }

    private val probeThreads = AtomicInteger()

    private val executor: ExecutorService =
        Executors.newCachedThreadPool { r ->
            Thread(r, "hoglake-hoglite-healthz-${probeThreads.incrementAndGet()}").apply { isDaemon = true }
        }

    /** The backends sampled, for the gauge registration. */
    val backendIds: Set<UUID> get() = backends.keys

    /**
     * Start a probe of every backend that has none young in flight, and
     * return without waiting for any of them. The answers land in
     * [reachable]; the first tick after boot publishes NaN until they do.
     */
    fun sample() {
        backends.forEach { (id, backend) -> launch(id, backend) }
    }

    /**
     * The last answer for [id]: 1.0 when its standard pool answered the
     * most recent completed probe inside its deadline, 0.0 when it did
     * not, when the attempt in flight is already past its deadline, or
     * when the most recent attempt was abandoned; NaN before any probe
     * completed (or for an id that is not a configured backend).
     */
    fun reachable(id: UUID): Double {
        val backend = backends[id] ?: return Double.NaN
        synchronized(backend) {
            val current = backend.pending()
            if (current != null && backend.ageMs(current, System.nanoTime()) > backend.deadlineMs) return 0.0
            return backend.reachable
        }
    }

    /**
     * Attempts of [id] started and not finished — hung ones included.
     * Published as `hoglake_hoglite_health_probe_attempts_hung{backend}`:
     * 1 while a probe is in flight (including every normal probe), and
     * [MAX_CONCURRENT_PROBES] is the cap the class comment describes. 0
     * for an id that is not a configured backend.
     */
    fun outstandingAttempts(id: UUID): Int = backends[id]?.running?.get() ?: 0

    /** [outstandingAttempts] summed over every backend. */
    val outstandingAttempts: Int get() = backends.values.sumOf { it.running.get() }

    /**
     * How many probe TASKS [id] has started, as against how many times
     * [sample] was called: single flight is the difference.
     */
    internal fun launchedProbes(id: UUID): Long = backends.getValue(id).launched.get()

    private fun launch(
        id: UUID,
        backend: Backend,
    ) {
        synchronized(backend) {
            val current = backend.pending()
            val ageMs = current?.let { backend.ageMs(it, System.nanoTime()) }
            if (ageMs != null) {
                if (ageMs <= backend.deadlineMs) return
                backend.reachable = 0.0
            }
            if (backend.running.get() >= MAX_CONCURRENT_PROBES) {
                backend.reachable = 0.0
                if (backend.capped) return
                backend.capped = true
                // ERROR, as HealthProbe's refusal is: past this point the
                // sampler cannot recover on its own and the remedy is a
                // restart, so the line has to be one an alert can key on.
                // Once per stall, not per tick: the state is the gauge's.
                log.error {
                    "$MAX_CONCURRENT_PROBES hoglite health probe attempts of backend $id are hung and cannot be " +
                        "interrupted; hoglake_hoglite_backend_reachable reads 0 until one returns, or until " +
                        "this pod restarts (see hoglake_hoglite_health_probe_attempts_hung)"
                }
                return
            }
            if (backend.capped) {
                backend.capped = false
                log.warn { "a parked hoglite health probe attempt of backend $id returned; sampling resumes" }
            }
            if (ageMs != null) {
                log.warn {
                    "abandoning a hoglite health probe attempt of backend $id ${ageMs}ms old " +
                        "(deadline ${backend.deadlineMs}ms); reporting it unreachable and launching a fresh " +
                        "attempt so a hung one cannot pin hoglake_hoglite_backend_reachable at its last value"
                }
            }
            backend.running.incrementAndGet()
            backend.launched.incrementAndGet()
            val attempt = Attempt(System.nanoTime())
            attempt.task =
                executor.submit {
                    var answer = 0.0
                    try {
                        answer = if (probeQuietly(id)) 1.0 else 0.0
                    } finally {
                        // Under the lock, so it cannot interleave with a
                        // launch: an attempt the sampler has abandoned is no
                        // longer the one in flight, and its answer is stale.
                        // One past its deadline is a failed probe, whatever
                        // it says, even when no tick has replaced it yet.
                        // The decrement is in the same block as the publish:
                        // a tick between the two would find the answer taken
                        // and the attempt still counted, and with an
                        // abandoned attempt parked would take the cap branch,
                        // overwrite the fresh answer with 0 and log the
                        // restart ERROR for a backend that just answered.
                        synchronized(backend) {
                            if (backend.inFlight === attempt) {
                                val late = backend.ageMs(attempt, System.nanoTime()) > backend.deadlineMs
                                backend.reachable = if (late) 0.0 else answer
                                attempt.answered = true
                            }
                            backend.running.decrementAndGet()
                        }
                    }
                }
            backend.inFlight = attempt
        }
    }

    private fun probeQuietly(id: UUID): Boolean =
        runCatching { probe(id) }.getOrElse {
            log.warn(it) { "hoglite health probe of backend $id failed; reporting it unreachable" }
            false
        }

    /**
     * Stop probing. `shutdown()` and a bounded wait before `shutdownNow()`,
     * as `HealthProbe` does: an attempt blocked in the HTTP client is
     * left to its own timeout rather than interrupted for a stack nobody
     * needs to read.
     */
    override fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
            executor.shutdownNow()
        }
    }

    companion object {
        private const val SHUTDOWN_WAIT_SECONDS = 2L

        /**
         * Attempts that may be outstanding per backend: the one that is
         * hung and the replacement that has to be able to run, which is
         * `HealthProbe.MAX_CONCURRENT_PROBES` for the same reason.
         */
        internal const val MAX_CONCURRENT_PROBES = 2

        /**
         * The deadline for a backend's probe: `HogliteClient.reachable()`
         * is one attempt bounded by the backend's `connect_timeout_ms`
         * twice over (the connect, then the request), so an attempt older
         * than that is hung below those bounds.
         */
        fun deadlines(backends: Map<UUID, HogliteBackend>): Map<UUID, Long> =
            backends.mapValues { it.value.connectTimeoutMs * 2 }
    }
}
