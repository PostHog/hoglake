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
 * was `/healthz` saying ok while every `/v1` call HUNG; a hoglite call
 * cannot hang that way, because `HogliteClient` bounds every attempt
 * (connect, read, a retry budget) and breaks the circuit on a down
 * pool. A pod whose hoglite backend is down is a pod that answers
 * quickly and truthfully, which is what readiness asks.
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
 * backend's connect-plus-read bound, so waiting would have turned a
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
 *   of the pod's life. Abandoning publishes 0 — a probe that outlasted
 *   its deadline is a failed probe, which is how `HealthProbe` reads
 *   its own — so a backend whose probes hang cannot keep showing the 1
 *   its last completed probe left.
 * - **Only the attempt in flight publishes.** An abandoned attempt that
 *   finishes late drops its answer, so a stale false cannot overwrite
 *   the fresh attempt's true, or the reverse: the series carries the
 *   newest attempt's result.
 * - **A cap of [MAX_CONCURRENT_PROBES] outstanding attempts per
 *   backend.** With two already hung nothing more is started, so a
 *   permanent hang costs two parked threads per backend and never
 *   leaks more. The refusal publishes 0 too, and the parked attempts
 *   are `hoglake_hoglite_health_probe_attempts_hung{backend}`
 *   ([outstandingAttempts]): at the cap sampling resumes only when a
 *   parked attempt returns, so a cap that persists needs a restart.
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

    /** [seq] is the backend's launch count at launch, so a finished attempt can tell whether it was superseded. */
    private class Attempt(val seq: Long, val task: Future<*>, val startedNanos: Long)

    private class Backend(val deadlineMs: Long) {
        var inFlight: Attempt? = null
        val running = AtomicInteger()
        val launched = AtomicLong()

        /** The last answer: 1.0 reachable, 0.0 not, NaN before the first answer landed. */
        @Volatile
        var reachable: Double = Double.NaN
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
     * most recent completed probe, 0.0 when it did not or when the most
     * recent attempt was abandoned past its deadline, NaN before any
     * probe completed (or for an id that is not a configured backend).
     */
    fun reachable(id: UUID): Double = backends[id]?.reachable ?: Double.NaN

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
            val current = backend.inFlight
            if (current != null && !current.task.isDone) {
                val ageMs = (System.nanoTime() - current.startedNanos) / 1_000_000
                if (ageMs <= backend.deadlineMs) return
                log.warn {
                    "abandoning a hoglite health probe attempt of backend $id ${ageMs}ms old " +
                        "(deadline ${backend.deadlineMs}ms); reporting it unreachable and launching a fresh " +
                        "attempt so a hung one cannot pin hoglake_hoglite_backend_reachable at its last value"
                }
                backend.reachable = 0.0
            }
            if (backend.running.get() >= MAX_CONCURRENT_PROBES) {
                // ERROR, as HealthProbe's refusal is: past this point the
                // sampler cannot recover on its own and the remedy is a
                // restart, so the line has to be one an alert can key on.
                log.error {
                    "$MAX_CONCURRENT_PROBES hoglite health probe attempts of backend $id are hung and cannot be " +
                        "interrupted; hoglake_hoglite_backend_reachable reads 0 until one returns, or until " +
                        "this pod restarts (see hoglake_hoglite_health_probe_attempts_hung)"
                }
                backend.reachable = 0.0
                return
            }
            backend.running.incrementAndGet()
            val seq = backend.launched.incrementAndGet()
            val task =
                executor.submit {
                    try {
                        val answer = if (probeQuietly(id)) 1.0 else 0.0
                        // Under the lock, so it cannot interleave with a
                        // launch: an attempt the sampler has abandoned is no
                        // longer the one in flight, and its answer is stale.
                        synchronized(backend) {
                            if (backend.inFlight?.seq == seq) backend.reachable = answer
                        }
                    } finally {
                        backend.running.decrementAndGet()
                    }
                }
            backend.inFlight = Attempt(seq, task, System.nanoTime())
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
