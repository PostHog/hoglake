package com.posthog.hoglake

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

/**
 * `/healthz`'s own connection to Postgres (#218).
 *
 * ## What the probe is for, and what it must stop saying
 *
 * The probe pings the database on purpose: the zombie-server incident
 * was `/healthz` answering ok while every `/v1` call hung on a dead
 * Postgres, and that property is kept here — a database that does not
 * answer is a 503, within the kubelet's probe budget.
 *
 * What it must STOP saying is "unhealthy" when the request path is
 * merely busy. The old handler was `jdbi.withHandle { SELECT 1 }`
 * against the REQUEST pool, so a commit convoy that filled the pool
 * made the probe wait on `HikariPool.getConnection` behind 10 commits
 * each holding a connection across a 30 s advisory-lock wait. "The pool
 * is busy serving commits" is not "the database is dead", and the old
 * probe could not tell them apart — so liveness restarted the pods, and
 * the restart discarded every queued commit and made the convoy worse.
 *
 * ## How it stays independent
 *
 * - **Its own pool, of exactly one connection**, so nothing the request
 *   path does can take the probe's connection or make it queue. One and
 *   not more because the question is binary and a second connection
 *   would only let two probes be wrong at once.
 * - **Its own threads — two of them**, so it does not enter the
 *   blocking request dispatcher either (`api/BlockingDispatch.kt`
 *   bypasses `/healthz` for the same reason), and so a probe stuck in
 *   JDBC is not stuck on a thread anything else needs. TWO and not one
 *   because of what recovery requires: when a hung attempt is abandoned
 *   the replacement must be able to RUN, and behind a single thread it
 *   would merely queue behind the corpse and time out in its turn,
 *   forever. Two is also the whole cap — see the single-flight note.
 * - **Four layered bounds, all from `HOGLAKE_HEALTH_PROBE_TIMEOUT_MS`**:
 *   Hikari's `connectionTimeout`, pgjdbc's `connectTimeout` and
 *   `socketTimeout`, and the session `statement_timeout`. A probe can
 *   fail at any of the four and each of them answers inside the budget.
 *   `initializationFailTimeout = -1` so the pool is CONSTRUCTED even
 *   while the database is down: the answer is still 503 either way (the
 *   probe catches), but with Hikari's fail-fast default the constructor
 *   throws, the `lazy` does not memoize, and every probe of an outage
 *   builds and discards a whole pool.
 * - **A wall-clock deadline** of twice the per-operation bound (connect,
 *   then query) as the backstop for anything the four miss. It is
 *   enforced by awaiting a task that runs in this class's OWN scope, not
 *   as a child of the caller: `withTimeoutOrNull` around a blocking JDBC
 *   call cannot interrupt it — a blocking call has no suspension point
 *   to deliver cancellation to — so the deadline has to be able to
 *   ABANDON the task and answer without it.
 * - **Single flight, WITH AN AGE BOUND.** A probe already in flight is
 *   joined rather than duplicated, so a slow database produces one
 *   attempt and not one per kubelet tick. But joining on "still
 *   running" alone is a trap: a task hung below its JDBC bounds stays
 *   running forever, every later probe joins it, and `/healthz`
 *   reports 503 for the rest of the pod's life even after Postgres
 *   comes back — a permanently unready pod, which is the same outage
 *   in a different costume. So an attempt is joined only while it is
 *   YOUNGER THAN ITS OWN DEADLINE; past that it is ABANDONED and a
 *   fresh attempt is launched on the second thread. Abandoned attempts
 *   are capped at [MAX_CONCURRENT_PROBES]: with two already hung the
 *   probe refuses to launch a third and answers 503 directly, so a
 *   permanent hang costs two parked threads and never leaks more.
 *
 * The pool is built LAZILY, on the first probe. Thirty-odd test classes
 * construct an `App` and never call `/healthz`; none of them should open
 * a second connection, and several point `Config` at no database at all.
 */
class HealthProbe(
    private val perOperationTimeoutMs: Long,
    dataSource: () -> DataSource,
) : AutoCloseable {
    private val log = KotlinLogging.logger {}

    /**
     * The whole-probe bound: one connect plus one query, each already
     * bounded by [perOperationTimeoutMs]. At the 2 s default that is 4 s,
     * inside the kubelet's usual 5 s `timeoutSeconds`.
     */
    val deadlineMs: Long = perOperationTimeoutMs * 2

    private val source = lazy(dataSource)

    /**
     * [MAX_CONCURRENT_PROBES] threads, and the code never launches more
     * attempts than that, so the queue is always empty and a fresh
     * attempt after an abandonment RUNS rather than waiting behind the
     * one it replaced. (A `ThreadPoolExecutor` with an unbounded queue
     * never grows past its core size, which is why core and max are
     * both the cap rather than 1 and 2.)
     */
    private val executor =
        ThreadPoolExecutor(
            MAX_CONCURRENT_PROBES,
            MAX_CONCURRENT_PROBES,
            KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            LinkedBlockingQueue(),
            // Numbered from a counter of its own. Naming them off
            // `launched` gave two threads the same name whenever an
            // attempt was joined rather than launched, which is most of
            // the time — and a thread dump is the tool this class
            // exists to be read with.
            { r -> Thread(r, "hoglake-healthz-${probeThreads.incrementAndGet()}").apply { isDaemon = true } },
        ).apply { allowCoreThreadTimeOut(true) }

    private val scope = CoroutineScope(SupervisorJob() + executor.asCoroutineDispatcher())

    /** An attempt and when it started, so [reachable] can judge its age. */
    private class Attempt(val task: Deferred<Boolean>, val startedNanos: Long)

    private var inFlight: Attempt? = null

    private val launched = java.util.concurrent.atomic.AtomicLong()

    /**
     * Attempts started and not yet finished — hung ones included.
     * Published as `hoglake_health_probe_attempts_hung`, because at
     * [MAX_CONCURRENT_PROBES] this is an ABSORBING STATE: both threads
     * are parked on connection attempts that cannot be interrupted, and
     * from then on the probe answers 503 without launching anything,
     * which looks exactly like a dead database.
     */
    private val running = java.util.concurrent.atomic.AtomicInteger()

    /** For the gauge: see [running]. 2 with a live database = restart the pod. */
    val outstandingAttempts: Int get() = running.get()

    private val probeThreads = java.util.concurrent.atomic.AtomicInteger()

    /**
     * How many probe TASKS have been started, as against how many times
     * [reachable] has been called. Single flight is the difference, and
     * this is how a test sees it: an outage that outlasts a probe's
     * deadline must not leave one queued task per kubelet tick behind
     * the one stuck thread — the queue would drain through every stale
     * attempt before a fresh one ran, so the pod would keep answering
     * 503 long after Postgres came back.
     */
    internal val launchedProbes: Long get() = launched.get()

    /**
     * True iff Postgres answered `SELECT 1` on the probe's own
     * connection within [deadlineMs].
     *
     * NEVER THROWS, and the contract is load-bearing twice over: the
     * handler has no error branch, and an exception escaping here would
     * reach StatusPages as a 500 — a status the readiness probe reads
     * the same as a 503 but which says something false about why. Every
     * failure mode is the same answer: a refused connection, a socket
     * timeout, a statement timeout, a deadline the four JDBC bounds
     * outlasted, and an attempt cancelled because the probe was closed
     * under a shutting-down process. The one exception it does
     * propagate is cancellation of the CALLER, which belongs to
     * structured concurrency and is not this function's to swallow.
     */
    suspend fun reachable(): Boolean {
        val attempt = claimAttempt() ?: return refuse()
        val answer =
            try {
                withTimeoutOrNull(deadlineMs) { attempt.task.await() }
            } catch (e: CancellationException) {
                // Ours to report only if the CALLER is still alive; if
                // the caller was cancelled, the exception is its own.
                if (!currentCoroutineContext().isActive) throw e
                log.warn(e) { "health probe attempt was cancelled; reporting the catalog unreachable" }
                null
            } catch (e: Throwable) {
                log.warn(e) { "health probe failed; reporting the catalog unreachable" }
                null
            }
        if (answer == null) {
            log.warn {
                "health probe exceeded its ${deadlineMs}ms deadline; reporting the catalog unreachable"
            }
        }
        return answer ?: false
    }

    /**
     * The attempt this probe should await: the one in flight if it is
     * still YOUNGER than a deadline, a fresh one otherwise — or null
     * when [MAX_CONCURRENT_PROBES] are already hung, which is the
     * answer "unreachable" without launching anything.
     */
    private fun claimAttempt(): Attempt? =
        synchronized(this) {
            val current = inFlight
            if (current != null && current.task.isActive) {
                val ageMs = (System.nanoTime() - current.startedNanos) / 1_000_000
                if (ageMs <= deadlineMs) return@synchronized current
                log.warn {
                    "abandoning a health probe attempt ${ageMs}ms old (deadline ${deadlineMs}ms); " +
                        "launching a fresh one so a hung attempt cannot pin /healthz at 503"
                }
            }
            if (running.get() >= MAX_CONCURRENT_PROBES) return@synchronized null
            running.incrementAndGet()
            launched.incrementAndGet()
            val task = scope.async { probe() }
            task.invokeOnCompletion { running.decrementAndGet() }
            Attempt(task, System.nanoTime()).also { inFlight = it }
        }

    private fun refuse(): Boolean {
        // ERROR, not WARN, and it is the one line in this class that
        // earns it: past this point the probe cannot recover on its
        // own. Both threads are parked on connection attempts nothing
        // can interrupt, every later probe takes this branch, and
        // /healthz reports 503 for the rest of the pod's life whatever
        // Postgres does. The remedy is a restart, so the line has to be
        // one an alert can key on.
        log.error {
            "$MAX_CONCURRENT_PROBES health probe attempts are hung and cannot be interrupted; " +
                "/healthz will report the catalog unreachable until this pod is restarted " +
                "(see hoglake_health_probe_attempts_hung)"
        }
        return false
    }

    private fun probe(): Boolean =
        runCatching {
            source.value.connection.use { c ->
                c.createStatement().use { st ->
                    st.executeQuery("SELECT 1").use { it.next() }
                }
            }
        }.getOrElse {
            log.warn(it) { "health probe failed: the catalog database did not answer" }
            false
        }

    /**
     * Stop probing and release the connection.
     *
     * `scope.cancel()` first, so an attempt that has not started is
     * never started; an attempt already blocked in JDBC cannot be
     * interrupted by cancellation and is left to the daemon thread and
     * the JVM's exit. `shutdown()` and a bounded wait rather than
     * `shutdownNow()`, for the same reason the request dispatcher uses
     * one: interrupting a live statement to save a few milliseconds of
     * shutdown buys nothing and produces a confusing stack.
     *
     * NOT SAFE AGAINST A CONCURRENT FIRST PROBE, and deliberately not
     * made so. `source.isInitialized()` can be false here while another
     * thread is inside the `lazy` initializer, in which case the pool
     * that thread builds is never closed — one leaked Hikari pool at
     * the end of a process's life. The alternative is holding the
     * `lazy`'s monitor across a pool construction that can take the
     * whole connect timeout, inside a shutdown hook the kubelet is
     * already counting down. Closing is called once, from the shutdown
     * hook, after the engine has stopped accepting, so the window needs
     * a probe that arrived in the last milliseconds of the process; the
     * JVM is about to reclaim it either way.
     */
    override fun close() {
        scope.cancel()
        executor.shutdown()
        if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
            executor.shutdownNow()
        }
        if (source.isInitialized()) (source.value as? AutoCloseable)?.close()
    }

    companion object {
        private const val KEEP_ALIVE_SECONDS = 60L
        private const val SHUTDOWN_WAIT_SECONDS = 2L

        /**
         * Attempts that may be outstanding at once — which is also the
         * thread count, so the cap is enforced by arithmetic and by the
         * pool alike.
         *
         * TWO: one for the attempt that is hung and one for the
         * replacement that has to be able to run, which is the minimum
         * that makes recovery from a hang possible at all. A third
         * would buy nothing — if two consecutive attempts are both hung
         * past their deadline the database is not answering, and that
         * IS the 503 — while every extra slot is a thread a permanent
         * hang parks for the life of the process.
         */
        internal const val MAX_CONCURRENT_PROBES = 2

        /** Build the probe production uses: one Hikari connection of its own. */
        fun fromConfig(cfg: Config): HealthProbe = HealthProbe(cfg.healthProbeTimeoutMs) { pool(cfg) }

        /**
         * The one-connection pool, separate from `Database.dataSource`'s.
         *
         * THIS IS A SECOND SESSION DEFINITION, and deliberately not
         * [com.posthog.hoglake.Database.SESSION_INIT_SQL]. That constant
         * is the one definition of what a REQUEST session carries — a
         * 60 s `statement_timeout` and a 30 s
         * `idle_in_transaction_session_timeout` — and neither number is
         * right here: a probe that took 60 s would have been counted
         * dead a dozen times over, so the statement bound is
         * `HOGLAKE_HEALTH_PROBE_TIMEOUT_MS` instead, twelve times
         * shorter. The divergence is the point, not an oversight; what
         * would be a bug is this drifting from the PROBE's own knob,
         * which it cannot, because it is that knob.
         *
         * No `idle_in_transaction_session_timeout` at all, and nothing
         * is lost: the probe never opens a transaction. `SELECT 1` runs
         * on an autocommit connection borrowed and returned inside one
         * `use` block, so there is no idle-in-transaction state for a
         * bound to kill. The request pool needs one because commits and
         * the cleanup drain hold transactions open across object-store
         * round trips (README.md §7, and `RemovalStore`'s call bounds
         * derive from it).
         *
         * `connectTimeout`/`socketTimeout` are pgjdbc CONNECTION
         * PROPERTIES in SECONDS, which is why they are rounded up to a
         * whole second and floored at one: that granularity is the
         * reason `HOGLAKE_HEALTH_PROBE_TIMEOUT_MS` is documented as
         * having a one-second floor on those two layers.
         */
        fun pool(cfg: Config): HikariDataSource {
            val ms = cfg.healthProbeTimeoutMs
            val seconds = ((ms + 999) / 1000).coerceAtLeast(1)
            return HikariDataSource(
                HikariConfig().apply {
                    jdbcUrl = cfg.jdbcUrl
                    username = cfg.dbUser
                    password = cfg.dbPassword
                    maximumPoolSize = 1
                    minimumIdle = 1
                    poolName = "hoglake-healthz"
                    connectionTimeout = ms
                    validationTimeout = ms
                    // Build the pool even when nothing answers: see the
                    // class comment's initializationFailTimeout note.
                    initializationFailTimeout = -1
                    connectionInitSql = "SET statement_timeout = '${ms}ms'"
                    addDataSourceProperty("connectTimeout", seconds)
                    addDataSourceProperty("socketTimeout", seconds)
                    // So `pg_stat_activity` names the probe's session.
                    addDataSourceProperty("ApplicationName", "hoglake-healthz")
                },
            )
        }
    }
}
