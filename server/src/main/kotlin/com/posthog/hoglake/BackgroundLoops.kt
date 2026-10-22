package com.posthog.hoglake

import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull

/**
 * The one home for every periodic background loop (hydrator sweep,
 * expiry, cleanup drain, compaction, metrics sampler): coroutines under
 * a single [SupervisorJob]-rooted scope owned by the App lifecycle,
 * replacing the per-service daemon threads.
 *
 * Contracts:
 *  - `intervalMs <= 0` = loop disabled ([register] is a no-op), exactly
 *    as the old per-service startLoop behaved; tests keep driving the
 *    services' runOnce/sampleOnce entry points directly.
 *  - Per-loop isolation: one iteration's exception is caught INSIDE the
 *    loop — logged, counted (`hoglake_background_loop_failures_total{loop}`),
 *    and the loop sleeps its interval and runs again. The supervisor
 *    root is belt-and-braces: even an exception that somehow escaped a
 *    child could not cancel the siblings.
 *  - Loop bodies are blocking (JDBC/S3), so they run on [Dispatchers.IO]
 *    inside [runInterruptible]: cancellation interrupts a mid-body
 *    blocking call instead of waiting it out.
 *  - [close] is structured shutdown: cancel the supervisor, then join
 *    every loop with a hard [SHUTDOWN_TIMEOUT_MS] bound — shutdown may
 *    be slow to let an iteration finish, but can never hang.
 */
class BackgroundLoops : AutoCloseable {
    private val log = KotlinLogging.logger {}
    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor + Dispatchers.IO)
    private val loops = mutableListOf<Job>()

    /**
     * Called with (loop name, delay ms) before every between-iterations
     * sleep, so a test can read the delays a loop ACTUALLY takes — the
     * only way to pin that an unjittered loop sleeps exactly its
     * interval, which is what [Jitter]'s "only compaction" argument
     * rests on. Default no-op; nothing but a test sets it.
     */
    internal var onDelay: (String, Long) -> Unit = { _, _ -> }

    /**
     * Register a periodic loop: run [body], sleep [intervalMs], repeat.
     * [intervalMs] <= 0 registers nothing (the disabled contract).
     *
     * [jitter], when given, makes each sleep a draw from
     * `intervalMs x (1 ± fraction)` instead of exactly [intervalMs]; see
     * [Jitter]. Null — every loop but compaction — keeps the sleep
     * exact.
     */
    fun register(
        name: String,
        intervalMs: Long,
        jitter: Jitter? = null,
        body: () -> Unit,
    ) {
        if (intervalMs <= 0) return
        val nextDelay: () -> Long =
            if (jitter == null) {
                { intervalMs }
            } else {
                val random = java.util.Random(jitter.seed)
                ({ jitteredDelayMs(intervalMs, jitter.fraction, random.nextDouble()) })
            }
        loops +=
            scope.launch(CoroutineName(name)) {
                while (true) {
                    try {
                        runInterruptible { body() }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        // One bad iteration never stops the loop (or any
                        // other loop): log, count, run again next interval.
                        log.error(e) { "background loop '$name' iteration failed; continuing" }
                        Metrics.backgroundLoopFailure(name)
                    }
                    val ms = nextDelay()
                    onDelay(name, ms)
                    delay(ms)
                }
            }
    }

    /** Cancel every loop and wait (bounded) for them to finish. Idempotent. */
    override fun close() {
        supervisor.cancel()
        val clean =
            runBlocking {
                withTimeoutOrNull(SHUTDOWN_TIMEOUT_MS) { loops.joinAll() } != null
            }
        if (!clean) {
            log.warn { "background loops did not stop within ${SHUTDOWN_TIMEOUT_MS}ms; abandoning" }
        }
    }

    /**
     * A loop's sleep jitter: each delay is drawn uniformly from
     * `[interval x (1 - fraction), interval x (1 + fraction)]`, from a
     * [java.util.Random] seeded with [seed].
     *
     * # Why compaction has one and nothing else does
     *
     * gigahog-prod-us, 2026-10-21: nine maintenance pods ran the
     * compaction loop at `HOGLAKE_COMPACTION_INTERVAL_MS=30000` with an
     * exact sleep. Six sweeps on one catalog started within two seconds,
     * planned one after another under the catalog planning lock, ran
     * their groups, FINISHED together — and so, 30 s later, re-planned
     * together: nine concurrent copies of millpond's expensive day-bucket
     * probes on a database that was also rebuilding an index. Every
     * probe crossed the 15 s plan timeout, every pod failed, every pod
     * slept exactly 30 s and failed again, in step: 19 failures in nine
     * minutes. An exact sleep is a phase lock: whatever lines the pods up
     * once keeps them lined up forever. A random sleep lets a herd
     * disperse at `fraction x interval` per iteration.
     *
     * Nothing else gets one, and the reasons are specific. The
     * maintenance sampler's TICK CADENCE is load-bearing — its batch
     * size and interval together are the scan rate the README sizes
     * against retention. Expiry's interval is documented arithmetic
     * (`HOGLAKE_EXPIRY_BATCH x 60 / interval` must exceed the commit
     * rate). The rest run on one maintenance pod, or under their own
     * claims and locks, and no herd has been seen on them.
     *
     * The SEED is the pod identity's hash, so a pod's sequence is
     * reproducible from its name and two pods' sequences differ. The
     * mean delay is the interval, so the loop's rate is unchanged.
     */
    data class Jitter(
        val fraction: Double,
        val seed: Long,
    ) {
        init {
            require(fraction >= 0.0 && fraction < 1.0) { "jitter fraction must be in [0, 1), got $fraction" }
        }
    }

    companion object {
        /** Hard bound on shutdown: never hang the process on a stuck loop. */
        const val SHUTDOWN_TIMEOUT_MS = 5_000L

        /** The compaction loop's [Jitter.fraction]: ±25% of the interval. */
        const val COMPACTION_JITTER_FRACTION = 0.25

        /**
         * One jittered delay: [intervalMs] scaled by `1 + fraction x
         * (2u - 1)` for [u] in `[0, 1)`, so the result is in
         * `[intervalMs x (1 - fraction), intervalMs x (1 + fraction)]`
         * and is never below 1 ms.
         */
        fun jitteredDelayMs(
            intervalMs: Long,
            fraction: Double,
            u: Double,
        ): Long {
            val scaled = intervalMs * (1.0 + fraction * (2.0 * u - 1.0))
            return maxOf(1L, Math.round(scaled))
        }
    }
}
