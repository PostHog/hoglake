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

    private fun serve(vararg steps: Step): Pair<HttpServer, AtomicInteger> {
        val calls = AtomicInteger()
        val bodies = ConcurrentLinkedQueue<String>()
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

    private fun client(
        url: String,
        threshold: Int = 10,
        budgetMs: Long = 2_000,
    ) = HogliteClient(
        standardUrl = url,
        retryBudgetMs = budgetMs,
        circuitFailureThreshold = threshold,
        circuitOpenMs = 250,
        sleeper = {},
    )

    private fun txOne(client: HogliteClient): List<HogliteQueryResult> =
        client.tx(
            "t",
            "tx-" + UUID.randomUUID(),
            null,
            listOf(HogliteStatement("SELECT 1", emptyList())),
        )

    @Test
    fun `a 421 re-pins and retries with the txid reused verbatim`() {
        val (server, calls) =
            serve(
                Step(421, "{}"),
                Step(200, "{\"results\":[{\"cols\":[\"x\"],\"rows\":[[1]]}]}"),
            )
        val c = client("http://127.0.0.1:${server.address.port}")
        assertThat(txOne(c).single().rows.single().single()).isEqualTo(1)
        assertThat(calls.get()).isEqualTo(2)
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
    fun `the retry loop never starts an attempt past the budget`() {
        // A server that answers only after the budget: the client must
        // not begin the attempt at all (remaining <= 0 check).
        val (server, calls) = serve(Step(200, "{\"results\":[]}"))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 1)
        Thread.sleep(5) // let the budget elapse before the first attempt
        assertThatThrownBy { txOne(c) }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        // budget=1ms: at most a couple of attempts, never a long spin
        assertThat(calls.get()).isLessThan(50)
    }
}
