package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The boot refusals #218 added, in `RetirementConfigTest`'s shape: facts
 * about constants and a `require` block, so they red in the fast lane
 * with no Docker.
 *
 * Every one of them guards a misconfiguration whose symptom appears far
 * from its cause — a server that accepts and never answers, a probe
 * bound Hikari silently replaced, a Netty constructor that throws after
 * migrations have already run, a fleet-wide 500 rate nobody can
 * attribute to a knob.
 */
class RequestDispatchConfigTest {
    @Test
    fun `a dispatcher with no threads is refused at boot`() {
        // A pool of zero threads accepts every request and serves none:
        // the Netty call thread hands the call over and the client waits
        // until it gives up. There is no reading of 0 that means "do not
        // dispatch" — that is what PROBE_PATHS is for.
        //
        // MUTATION: delete the `require` and this reds — Config builds,
        // and the server hangs on its first request instead.
        assertThatThrownBy { Config(requestThreads = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_REQUEST_THREADS")
            .hasMessageContaining("HOGLAKE_DB_POOL_SIZE")
        assertThatThrownBy { Config(requestThreads = -4) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `more handler threads than pool connections is refused, naming both knobs`() {
        // Handler threads past the pool do not buy concurrency for a
        // handler whose first act is to borrow a connection: they queue
        // inside HikariPool.getConnection and 500 after its 5 s bound,
        // instead of queueing in the dispatcher where the wait is
        // measured and shed with a typed 503. Raising the dispatcher is
        // a PAIR of knobs and this makes forgetting the second one a
        // named boot failure.
        //
        // MUTATION: delete the `require` and this reds; the pod boots
        // and converts dispatcher queueing into Hikari timeouts.
        assertThatThrownBy { Config(dbPoolSize = 10, requestThreads = 11) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_REQUEST_THREADS")
            .hasMessageContaining("HOGLAKE_DB_POOL_SIZE")

        // Equal is the DEFAULT and must stay legal, and so must under.
        Config(dbPoolSize = 10, requestThreads = 10)
        Config(dbPoolSize = 10, requestThreads = 6)
    }

    @Test
    fun `the dispatcher defaults to the database pool size`() {
        // Not a restated literal: the default is the other property, so
        // the two cannot drift.
        //
        // MUTATION: give HOGLAKE_REQUEST_THREADS a literal default and
        // this reds for any non-default pool size.
        assertThat(Config().requestThreads).isEqualTo(Config().dbPoolSize)
        assertThat(Config(dbPoolSize = 7).requestThreads).isEqualTo(7)
    }

    @Test
    fun `a probe timeout below Hikari's own floor is refused`() {
        // Hikari refuses a connectionTimeout under 250 ms and
        // substitutes its 30 s DEFAULT — six times the kubelet's probe
        // timeout — so the number in the values file would stop being
        // the number in force, silently.
        //
        // MUTATION: delete the `require` and this reds.
        assertThatThrownBy { Config(healthProbeTimeoutMs = 249) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_HEALTH_PROBE_TIMEOUT_MS")
        // The floor itself is legal, and so is the default.
        Config(healthProbeTimeoutMs = Config.MIN_HEALTH_PROBE_TIMEOUT_MS)
        assertThat(Config().healthProbeTimeoutMs).isGreaterThanOrEqualTo(Config.MIN_HEALTH_PROBE_TIMEOUT_MS)
    }

    @Test
    fun `a netty call group of zero is refused before Netty can throw about it`() {
        // Netty's own refusal is an IllegalArgumentException from inside
        // its constructor naming neither the knob nor the value, and it
        // arrives AFTER migrations have run.
        //
        // MUTATION: delete the `require` and this reds.
        assertThatThrownBy { Config(nettyCallGroupSize = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_NETTY_CALL_GROUP_SIZE")
    }

    @Test
    fun `the netty call group default is a floor over Ktor's own sizing, and the worker group is untouched`() {
        // Ktor sizes the CALL group at availableProcessors exactly; the
        // default here is a max() over that same number, so it only ever
        // raises the small end. The WORKER group keeps Ktor's default
        // (parallelism / 2 + 1), which this must not have a knob for —
        // a floor shaped like this one would RAISE it on every pod with
        // more than two CPUs.
        //
        // MUTATION: make DEFAULT_NETTY_GROUP_SIZE a bare literal 4 and
        // the availableProcessors assertion reds on any machine with
        // more than four cores — which is every CI runner.
        val cpus = Runtime.getRuntime().availableProcessors()
        assertThat(Config.DEFAULT_NETTY_GROUP_SIZE).isEqualTo(maxOf(4, cpus))
        assertThat(Config.DEFAULT_NETTY_GROUP_SIZE).isGreaterThanOrEqualTo(cpus)
        assertThat(Config().nettyCallGroupSize).isEqualTo(Config.DEFAULT_NETTY_GROUP_SIZE)
        assertThat(Config::class.java.declaredFields.map { it.name })
            .describedAs("the worker group keeps Ktor's default; a knob here would raise it fleet-wide")
            .doesNotContain("nettyWorkerGroupSize")
    }
}
