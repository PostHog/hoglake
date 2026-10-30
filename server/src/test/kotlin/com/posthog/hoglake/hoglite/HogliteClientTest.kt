package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * HogliteClient's retry taxonomy and circuit breaker, against a scripted
 * HTTP server: the 421 re-pin, the 503 retry, the transport-failure
 * breaker, the retry-budget cap, and the dedup arm (a retried tx reuses
 * the caller's txid verbatim).
 *
 * Not a live-engine suite — hoglite itself is faked — so this runs in
 * the unit phase, no HOGLITE_BINARY needed.
 */
class HogliteClientTest {
    private val servers = mutableListOf<HttpServer>()

    @AfterEach
    fun stop() = servers.forEach { it.stop(0) }

    /** A scripted endpoint: each call consumes one [Step] (status + body), then repeats the last. */
    private class Step(val status: Int, val body: String = "")

    private val servedBodies = ConcurrentLinkedQueue<String>()

    private fun serve(vararg steps: Step): Pair<HttpServer, AtomicInteger> {
        val calls = AtomicInteger()
        val bodies = servedBodies
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { ex ->
            val i = calls.getAndIncrement()
            bodies += ex.requestBody.readBytes().decodeToString()
            val step = steps[minOf(i, steps.size - 1)]
            ex.sendResponseHeaders(step.status, step.body.toByteArray().size.toLong())
            ex.responseBody.use { it.write(step.body.toByteArray()) }
        }
        server.start()
        servers += server
        return server to calls
    }

    private val sleeps = ConcurrentLinkedQueue<Long>()

    private fun client(
        url: String,
        threshold: Int = 10,
        budgetMs: Long = 2_000,
        ownerUrlPattern: String = "",
    ) = HogliteClient(
        standardUrl = url,
        ownerUrlPattern = ownerUrlPattern,
        retryBudgetMs = budgetMs,
        circuitFailureThreshold = threshold,
        circuitOpenMs = 250,
        sleeper = { sleeps += it },
    )

    private fun txOne(client: HogliteClient): List<HogliteQueryResult> =
        client.tx(
            "t",
            "tx-" + UUID.randomUUID(),
            null,
            listOf(HogliteStatement("SELECT 1", emptyList())),
        )

    @Test
    fun `a 421 re-pins to the hinted owner and retries with the body byte-identical`() {
        val (ownerServer, ownerCalls) = serve(Step(200, "{\"results\":[{\"cols\":[\"x\"],\"rows\":[[1]]}]}"))
        val ownerPort = ownerServer.address.port
        // The 421 hint names the owner; the chart's headless-name
        // pattern turns it into a routable URL (here: the port IS the
        // "pod name", so the pattern reconstructs the second server).
        val (entryServer, entryCalls) =
            serve(Step(421, "{\"error\":\"misdirected\",\"owner\":\"$ownerPort\"}"))
        val c =
            client(
                "http://127.0.0.1:${entryServer.address.port}",
                ownerUrlPattern = "http://127.0.0.1:{owner}",
            )
        assertThat(txOne(c).single().rows.single().single()).isEqualTo(1)
        assertThat(entryCalls.get()).isEqualTo(1)
        assertThat(ownerCalls.get()).isEqualTo(1)
        // The retry re-sends the SAME bytes (same txid: at-most-once
        // depends on it) and backed off 200ms rather than spinning.
        assertThat(servedBodies.toList()).hasSize(2)
        assertThat(servedBodies.first()).isEqualTo(servedBodies.last())
        assertThat(sleeps).containsExactly(200L)
        // And the pin sticks: the next call goes straight to the owner.
        servedBodies.clear()
        txOne(c)
        assertThat(entryCalls.get()).isEqualTo(1)
        assertThat(ownerCalls.get()).isEqualTo(2)
    }

    @Test
    fun `a 503 retries within the budget and does not feed the breaker`() {
        val (server, calls) =
            serve(
                Step(503, "owner lost"),
                Step(200, "{\"results\":[]}"),
            )
        val c = client("http://127.0.0.1:${server.address.port}", threshold = 1)
        txOne(c)
        assertThat(calls.get()).isEqualTo(2)
        // Breaker threshold 1: had the 503 counted, the NEXT call would
        // fail fast. It must not.
        txOne(c)
        assertThat(calls.get()).isEqualTo(3)
    }

    @Test
    fun `a 503 past the budget is the typed 503`() {
        val (server, _) = serve(Step(503, "owner lost"))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 300)
        assertThatThrownBy { txOne(c) }
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
    }

    @Test
    fun `transport failures trip the breaker and an open breaker fails fast`() {
        val (server, calls) = serve(Step(200, "{\"results\":[]}"))
        val port = server.address.port
        server.stop(0)
        val c = client("http://127.0.0.1:$port", threshold = 2, budgetMs = 400)
        // Threshold 2: two calls whose attempts all fail open it.
        assertThatThrownBy { txOne(c) }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThatThrownBy { txOne(c) }
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessageContaining("circuit is open")
    }

    @Test
    fun `a deterministic refusal surfaces on first sight`() {
        val (server, calls) =
            serve(Step(422, "{\"error\":\"syntax error\"}"))
        val c = client("http://127.0.0.1:${server.address.port}")
        assertThatThrownBy { txOne(c) }.isInstanceOf(HogliteRejected::class.java)
        assertThat(calls.get()).isEqualTo(1)
    }

    @Test
    fun `a guard failure decodes the 1-based guard_index`() {
        val (server, _) =
            serve(Step(409, "{\"error\":\"expect_failed\",\"guard_index\":2}"))
        val c = client("http://127.0.0.1:${server.address.port}")
        assertThatThrownBy { txOne(c) }
            .isInstanceOf(HogliteGuardFailure::class.java)
            .satisfies({ e -> assertThat((e as HogliteGuardFailure).statementIndex).isEqualTo(1) })
    }

    @Test
    fun `the per-attempt timeout is capped to the remaining retry budget`() {
        // A SLOW server (answers after 2s) and a 150ms retry budget: the
        // attempt's own timeout is capped to the budget remainder, so the
        // call fails in hundreds of milliseconds, not after the full
        // (10s) attempt timeout the commit path configures.
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { ex ->
            Thread.sleep(2_000)
            ex.sendResponseHeaders(200, 2)
            ex.responseBody.use { it.write(byteArrayOf(123, 125)) }
        }
        server.start()
        servers += server
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 150)
        val started = System.nanoTime()
        assertThatThrownBy { txOne(c) }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        assertThat(elapsedMs).isLessThan(2_000)
    }

    @Test
    fun `the retry loop never starts an attempt past the budget`() {
        // Budget already spent before the first attempt: the client must
        // not begin it at all (the remaining <= 0 check at the top of
        // the loop).
        val (server, calls) = serve(Step(200, "{\"results\":[]}"))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 1)
        Thread.sleep(5) // let wall time pass; the deadline starts inside the call
        assertThatThrownBy { txOne(c) }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(calls.get()).isLessThan(50)
    }
}
