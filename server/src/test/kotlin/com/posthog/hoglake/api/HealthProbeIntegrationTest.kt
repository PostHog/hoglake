package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.containers.PostgreSQLContainer
import java.net.InetAddress
import java.net.ServerSocket
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The zombie-server property, kept and sharpened (#218).
 *
 * `/healthz` pings the database on purpose — the zombie-server incident
 * was the probe answering ok while every `/v1` call hung on a dead
 * Postgres — so a dead database must still be a 503, and it must be a
 * 503 INSIDE THE PROBE BUDGET rather than a hang the kubelet times out
 * on (a timed-out probe and a 503 look the same to liveness, but only
 * one of them says why in the logs, and only one of them is bounded).
 *
 * The other half — a BUSY request pool reading as 200 — is
 * [RequestDispatchIntegrationTest], which needs a real engine to mean
 * anything. This class only needs a database it is allowed to kill, so
 * it runs its own container rather than `PgTestSupport`'s shared one.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// ORDERED, because the last test destroys the database the others need
// and JUnit's default method order is deterministic but arbitrary — a
// dependency that relies on it is a dependency nothing states.
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class HealthProbeIntegrationTest {
    private val container: PostgreSQLContainer<*> =
        PostgreSQLContainer(PgTestSupport.image)
            .withUsername(PgTestSupport.USER)
            .withPassword(PgTestSupport.PASSWORD)
            .also { it.start() }

    private val cfg =
        Config(
            hydratorIntervalMs = 0,
            metricsIntervalMs = 0,
            maintenanceSummaryIntervalMs = 0,
            jdbcUrl = container.jdbcUrl,
            dbUser = PgTestSupport.USER,
            dbPassword = PgTestSupport.PASSWORD,
            // Short, so the whole test stays well inside a probe budget
            // and the failure path is measurable rather than inferred.
            healthProbeTimeoutMs = 1_000,
        )
    private val ds = Database.dataSource(cfg).also { Database.migrate(it) }
    private val app = App.build(cfg, Database.jdbi(ds), dataSource = ds)

    @AfterAll
    fun tearDown() {
        app.close()
        ds.close()
        runCatching { container.stop() }
    }

    /**
     * (ii) 200 while Postgres answers; 503 within the probe budget once
     * it does not — and `/livez` stays 200 throughout, because a
     * database outage must not be a process-liveness failure.
     *
     * Ordered inside ONE test method rather than split in two, because
     * the second half destroys the fixture the first half needs and a
     * JUnit method order is not a dependency.
     *
     * MUTATION: return `true` from `HealthProbe.probe`'s `getOrElse`
     * and the status assertion reds — that IS the zombie-server
     * regression, `/healthz` saying ok over a database that is gone.
     *
     * The elapsed assertion is a bound, not a discriminator: a stopped
     * container refuses connections immediately, so it reds only on a
     * mutation that makes the probe WAIT (an unbounded deadline against
     * a hang). `HealthProbeDeadlineTest` is where that case is pinned,
     * with a fixture that hangs rather than refuses.
     */
    @Test
    @Order(3)
    fun `healthz is 200 while postgres answers and 503 within the budget once it does not`() =
        testApplication {
            application { app.module(this) }

            assertThat(client.get("/healthz").status).isEqualTo(HttpStatusCode.OK)
            assertThat(client.get("/healthz").bodyAsText()).isEqualTo("ok")

            container.stop()

            val started = System.nanoTime()
            val dead = client.get("/healthz")
            val elapsedMs = (System.nanoTime() - started) / 1_000_000

            assertThat(dead.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(dead.bodyAsText()).isEqualTo("db unreachable")
            assertThat(elapsedMs).isLessThanOrEqualTo(app.healthProbe.deadlineMs)

            // The probe going red must not take the process down with it.
            assertThat(client.get("/livez").status).isEqualTo(HttpStatusCode.OK)
        }

    /**
     * A probe pointed at an address that accepts and then says nothing
     * answers false rather than throwing out of the handler, and answers
     * it inside the deadline — the cheap half of the same contract, with
     * no container to kill. An exception escaping `reachable` would
     * reach StatusPages as a 500, which a readiness probe reads the same
     * as a 503 but which says something false about why.
     *
     * The fixture is a socket this test BINDS AND HOLDS, never
     * accepting. Holding it is the point: taking a port from
     * `ServerSocket(0)` and closing it to get "a port nothing listens
     * on" is a time-of-check/time-of-use race — the OS is free to hand
     * that port to anything, including another test in this JVM — and a
     * probe that then connected to a real service would answer 200 and
     * red this for a reason nobody could reproduce. Holding it also
     * makes the test STRONGER: the TCP connect succeeds into the
     * backlog and the silence that follows is what pgjdbc's
     * `socketTimeout` and Hikari's `connectionTimeout` have to bound,
     * rather than an instant `ECONNREFUSED` that would pass with no
     * timeouts configured at all.
     *
     * MUTATION: drop the `runCatching`/`getOrElse` from
     * `HealthProbe.probe` so the JDBC exception escapes — `reachable`
     * throws and this reds.
     *
     * MUTATION: drop `socketTimeout`/`connectionTimeout` from
     * `HealthProbe.pool` and the elapsed assertion reds — the probe
     * waits on the silence far past its deadline. (Against a merely
     * CLOSED port it would not: a refusal is instant however the pool
     * is configured, which is the other half of why this fixture holds
     * the socket.)
     */
    @Test
    @Order(1)
    fun `a probe at an address that never answers reports unreachable without throwing`() {
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { silent ->
            val deadCfg = cfg.copy(jdbcUrl = "jdbc:postgresql://127.0.0.1:${silent.localPort}/hoglake")
            HealthProbe.fromConfig(deadCfg).use { probe ->
                val started = System.nanoTime()
                val reachable = runBlocking { probe.reachable() }
                val elapsedMs = (System.nanoTime() - started) / 1_000_000
                assertFalse(reachable)
                assertThat(elapsedMs).isLessThanOrEqualTo(probe.deadlineMs)
            }
        }
    }

    /**
     * The probe's connection is its OWN: exhausting the request pool
     * leaves it answering. This is the unit-level statement of
     * [RequestDispatchIntegrationTest]'s first assertion, and it is
     * here because it needs no engine at all — only a pool with every
     * connection handed out.
     *
     * MUTATION: build the probe over the request `DataSource` (`ds`)
     * instead of its own and this blocks for Hikari's whole
     * `connectionTimeout` and then reds.
     */
    @Test
    @Order(2)
    fun `the probe answers while every request-pool connection is held`() {
        val held = (1..cfg.dbPoolSize).map { ds.connection }
        try {
            HealthProbe.fromConfig(cfg).use { probe ->
                val started = System.nanoTime()
                val reachable = runBlocking { probe.reachable() }
                val elapsedMs = (System.nanoTime() - started) / 1_000_000
                assertTrue(reachable)
                assertThat(elapsedMs).isLessThan(probe.deadlineMs)
            }
        } finally {
            held.forEach { it.close() }
        }
    }
}
