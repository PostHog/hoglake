package com.posthog.hoglake

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.PrintWriter
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.util.logging.Logger
import javax.sql.DataSource
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * What the probe does when the database does not answer AT ALL (#218) —
 * neither answering nor refusing, which is the one failure mode the four
 * JDBC-level bounds in `HealthProbe.pool` cannot cover.
 *
 * Those bounds handle everything Postgres and pgjdbc produce, and
 * [com.posthog.hoglake.api.HealthProbeIntegrationTest] exercises them
 * against a container it stops. What is left is a connection attempt
 * that hangs BELOW them — a black-holed route, a socket a runtime bound
 * but never answers, a driver bug — and that is what the wall-clock
 * deadline, the age-bounded single flight and the concurrency cap are
 * all for. No Docker needed: the hang is the fixture.
 */
class HealthProbeDeadlineTest {
    /**
     * MUTATION: drop the `withTimeoutOrNull` from `HealthProbe.reachable`
     * and await the task directly — this test hangs for the fixture's
     * whole sleep instead of answering, which is the liveness kill it
     * exists to prevent.
     *
     * MUTATION: make `reachable` return `true` on a timed-out probe
     * and the `assertFalse` reds — a probe that cannot answer must read
     * as unreachable, which is the zombie-server rule.
     */
    @Test
    fun `a connection attempt that never returns is reported unreachable inside the deadline`() {
        HealthProbe(perOperationTimeoutMs = 250) { Hanging() }.use { probe ->
            val started = System.nanoTime()
            val reachable = runBlocking { probe.reachable() }
            val elapsedMs = (System.nanoTime() - started) / 1_000_000

            assertFalse(reachable)
            assertThat(probe.deadlineMs).isEqualTo(500)
            // The deadline plus scheduling slack — and far below the
            // fixture's hang, which is what discriminates.
            assertThat(elapsedMs).isLessThan(HANG_MS / 2)
        }
    }

    /**
     * THE PROPERTY THAT MATTERS MOST HERE: a hung attempt must not pin
     * `/healthz` at 503 for the rest of the pod's life.
     *
     * Joining on "still running" alone does exactly that — the attempt
     * never completes, every later probe joins it, and the pod stays
     * unready long after Postgres came back, which is the same outage
     * wearing a different hat. An attempt is therefore joined only while
     * it is younger than its own deadline; past that it is abandoned and
     * a fresh one runs on the second thread.
     *
     * MUTATION: restore the `isActive`-only join (drop the age check in
     * `claimAttempt`) and the recovery assertion reds — the probe
     * reports unreachable forever against a database that is answering.
     *
     * MUTATION: give the probe ONE thread instead of
     * [HealthProbe.MAX_CONCURRENT_PROBES] and it reds too: the fresh
     * attempt is correctly launched but queues behind the corpse and
     * never runs, so abandoning it bought nothing.
     */
    @Test
    fun `a hung attempt is abandoned and the probe recovers when the database answers again`() {
        val source = Hanging()
        HealthProbe(perOperationTimeoutMs = 250) { source }.use { probe ->
            assertFalse(runBlocking { probe.reachable() }, "hung fixture must read unreachable")

            // Past the deadline, so the next probe judges the attempt
            // too old to join rather than merely "still running".
            Thread.sleep(probe.deadlineMs + 200)
            source.hanging = false

            val started = System.nanoTime()
            val recovered = runBlocking { probe.reachable() }
            val elapsedMs = (System.nanoTime() - started) / 1_000_000

            assertTrue(recovered, "the probe must recover once the database answers")
            assertThat(elapsedMs).isLessThan(probe.deadlineMs)
        }
    }

    /**
     * The cap. A permanently hung database must cost a fixed number of
     * parked threads, not one per kubelet tick: with
     * [HealthProbe.MAX_CONCURRENT_PROBES] attempts already hung the
     * probe refuses to launch another and answers unreachable directly.
     *
     * MUTATION: remove the `running.get() >= MAX_CONCURRENT_PROBES`
     * branch and the launch count climbs with every probe — a thread
     * leak proportional to the length of the outage.
     */
    @Test
    fun `a permanent hang parks a bounded number of attempts and no more`() {
        HealthProbe(perOperationTimeoutMs = 250) { Hanging() }.use { probe ->
            repeat(6) {
                assertFalse(runBlocking { probe.reachable() })
                // Age each attempt past its deadline so the NEXT probe
                // abandons it — which is what would launch another.
                Thread.sleep(probe.deadlineMs + 50)
            }
            assertThat(probe.launchedProbes)
                .describedAs("six probes against a permanent hang must not launch six attempts")
                .isEqualTo(HealthProbe.MAX_CONCURRENT_PROBES.toLong())
        }
    }

    /**
     * Probes that arrive while one is in flight and still YOUNG join it
     * rather than launching their own: one attempt for three calls.
     *
     * CONCURRENT, and it has to be. A sequential probe does not return
     * until its own deadline expires, so three of them span three
     * deadlines and the second and third correctly find the attempt too
     * OLD to join — they exercise abandonment, not joining. Real
     * probes overlap (a kubelet's liveness and readiness ticks, two
     * replicas of a monitor), which is the case this pins.
     *
     * Elapsed time cannot see the difference — a second attempt started
     * beside the first would answer on its own deadline either way —
     * which is why the assertion is on the launch COUNT.
     *
     * MUTATION: drop the join branch entirely (always
     * `scope.async { probe() }`) and the count is 2, the cap, not 1.
     */
    @Test
    fun `probes arriving while one is young join it instead of launching their own`() {
        HealthProbe(perOperationTimeoutMs = 1_500) { Hanging() }.use { probe ->
            val answers =
                runBlocking {
                    (1..3).map { async(Dispatchers.Default) { probe.reachable() } }.awaitAll()
                }
            answers.forEach { assertFalse(it) }
            assertThat(probe.launchedProbes).isEqualTo(1L)
        }
    }

    private companion object {
        const val HANG_MS = 30_000L

        /**
         * A DataSource whose connection attempt never returns while
         * [hanging], and answers instantly once it is cleared. The
         * "answers" branch is a stub connection, not a real one: what is
         * under test is the probe's own control flow, and a container
         * here would only make the hang harder to produce.
         */
        class Hanging : DataSource {
            @Volatile
            var hanging: Boolean = true

            override fun getConnection(): Connection {
                if (hanging) {
                    Thread.sleep(HANG_MS)
                    throw AssertionError("the fixture is supposed to outlast the deadline")
                }
                return answeringConnection()
            }

            override fun getConnection(
                username: String?,
                password: String?,
            ): Connection = connection

            override fun getLogWriter(): PrintWriter? = null

            override fun setLogWriter(out: PrintWriter?) = Unit

            override fun setLoginTimeout(seconds: Int) = Unit

            override fun getLoginTimeout(): Int = 0

            override fun getParentLogger(): Logger = Logger.getGlobal()

            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw UnsupportedOperationException()

            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }

        /**
         * The smallest thing `HealthProbe.probe` can succeed against:
         * `createStatement().executeQuery("SELECT 1").next()` is true
         * and every `close()` is a no-op.
         *
         * A dynamic proxy rather than a hand-written class because
         * `java.sql.Connection` has some fifty methods and this test
         * cares about four of them; a hand-written stub would be
         * hundreds of lines of `TODO()` in which the four that matter
         * were invisible.
         */
        fun answeringConnection(): Connection = stub(Connection::class.java)

        private fun <T> stub(type: Class<T>): T {
            val handler =
                InvocationHandler { proxy, method, args ->
                    when (method.name) {
                        "createStatement" -> stub(Statement::class.java)
                        "executeQuery" -> stub(ResultSet::class.java)
                        "next" -> true
                        "isClosed" -> false
                        "hashCode" -> System.identityHashCode(proxy)
                        "equals" -> proxy === args?.getOrNull(0)
                        "toString" -> "stub(${type.simpleName})"
                        else -> defaultValue(method.returnType)
                    }
                }
            @Suppress("UNCHECKED_CAST")
            return Proxy.newProxyInstance(type.classLoader, arrayOf(type), handler) as T
        }

        /** A primitive-returning method may never be answered with null. */
        private fun defaultValue(returnType: Class<*>): Any? =
            when (returnType) {
                Void.TYPE -> null
                java.lang.Boolean.TYPE -> false
                java.lang.Integer.TYPE -> 0
                java.lang.Long.TYPE -> 0L
                java.lang.Short.TYPE -> 0.toShort()
                java.lang.Byte.TYPE -> 0.toByte()
                java.lang.Double.TYPE -> 0.0
                java.lang.Float.TYPE -> 0.0f
                java.lang.Character.TYPE -> ' '
                else -> null
            }
    }
}
