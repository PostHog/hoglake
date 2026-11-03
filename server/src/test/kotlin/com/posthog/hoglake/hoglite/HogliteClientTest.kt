package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
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
        val (ownerServer, ownerCalls) =
            serve(
                Step(200, "{\"results\":[{\"cols\":[\"x\"],\"rows\":[[1]],\"affected\":1}]}"),
            )
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
                Step(200, "{\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}]}"),
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
        val (server, calls) = serve(Step(200, "{\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}]}"))
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
            serve(Step(422, "{\"error\":\"syntax error\",\"code\":\"refused\"}"))
        val c = client("http://127.0.0.1:${server.address.port}")
        assertThatThrownBy { txOne(c) }.isInstanceOf(HogliteRejected::class.java)
        assertThat(calls.get()).isEqualTo(1)
    }

    @Test
    fun `malformed successful write arrays are unknown with the original txid`() {
        for (body in listOf(
            "{}", "{\"results\":null}", "{\"results\":[]}", "not-json",
            "{\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}]} {}",
            "{\"results\":[{}]}", "{\"results\":[null]}", "{\"results\":[1]}",
            "{\"results\":[{\"cols\":[1],\"rows\":[],\"affected\":0}]}",
            "{\"results\":[{\"cols\":[\"a\"],\"rows\":[[1,2]],\"affected\":0}]}",
            "{\"results\":[{\"cols\":[],\"rows\":[1],\"affected\":0}]}",
            "{\"results\":[{\"cols\":[],\"rows\":[],\"affected\":0.5}]}",
            "{\"results\":[{\"cols\":[],\"rows\":[],\"affected\":9223372036854775808}]}",
            "{\"results\":[{\"cols\":[],\"rows\":[],\"affected\":0},{\"cols\":[],\"rows\":[],\"affected\":0}]}",
        )) {
            val (server, calls) = serve(Step(200, body))
            val thrown = catchThrowable { txFixed(client("http://127.0.0.1:${server.address.port}"), "codec-tx") }
            assertThat(thrown).describedAs(body).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            assertThat((thrown as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo("codec-tx")
            assertThat(calls.get()).isEqualTo(1)
        }
    }

    @Test
    fun `a duplicate successful write decodes the complete original result array`() {
        val (server, calls) =
            serve(
                Step(
                    200,
                    "{\"dup\":true,\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}," +
                        "{\"cols\":[\"snapshot\",\"schema\"],\"rows\":[[7,3]],\"affected\":0}]}",
                ),
            )
        val result =
            client("http://127.0.0.1:${server.address.port}").tx(
                "t",
                "full-dup",
                "full-fp",
                listOf(HogliteStatement("UPDATE kv SET v=1"), HogliteStatement("SELECT 7,3")),
            )
        assertThat(result).hasSize(2)
        assertThat(result[0].affected).isEqualTo(1)
        assertThat(result[1].rows).containsExactly(listOf(7, 3))
        assertThat(calls.get()).isEqualTo(1)
    }

    // ---- the outcome taxonomy: committed, not committed, unknown ----------

    private val outcomeUnknown =
        "{\"error\":\"commit outcome unknown (applied locally, durability unconfirmed): " +
            "pod lease write deadline passed\",\"code\":\"outcome_unknown\"}"

    private fun txFixed(
        client: HogliteClient,
        txid: String,
    ) = client.tx("t", txid, null, listOf(HogliteStatement("INSERT INTO kv VALUES (1)", emptyList())))

    @Test
    fun `a 504 outcome_unknown retries with the same txid and returns the commit`() {
        val (server, calls) =
            serve(
                Step(504, outcomeUnknown),
                Step(200, "{\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}],\"dup\":true}"),
            )
        val c = client("http://127.0.0.1:${server.address.port}")
        txFixed(c, "tx-fixed")
        assertThat(calls.get()).isEqualTo(2)
        assertThat(servedBodies.toList()).hasSize(2)
        assertThat(servedBodies.first()).isEqualTo(servedBodies.last()).contains("\"txid\":\"tx-fixed\"")
    }

    @Test
    fun `a 504 past the budget is outcome unknown, retryable and never a refusal`() {
        val (server, _) = serve(Step(504, outcomeUnknown))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 300)
        val thrown = catchThrowable { txFixed(c, "tx-504") }
        assertThat(thrown)
            .isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .isNotInstanceOf(HogliteRejected::class.java)
        assertThat((thrown as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo("tx-504")
    }

    @Test
    fun `once an attempt may have applied the call cannot end as not committed`() {
        // 504 first, then not_applied until the budget runs out: the later
        // answers say nothing about the first attempt
        val (server, _) =
            serve(Step(504, outcomeUnknown), Step(503, "{\"error\":\"lease lost\",\"code\":\"not_applied\"}"))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 300)
        assertThatThrownBy { txFixed(c, "tx-sticky") }.isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
    }

    @Test
    fun `sticky ambiguity prevents later final refusals from erasing an earlier attempt`() {
        val refusals =
            listOf(
                Step(409, "{\"error\":\"expect_failed\",\"guard_index\":2}"),
                Step(422, "{\"code\":\"refused\"}"),
                Step(400, "{}"),
                Step(404, "{}"),
                Step(413, "{\"code\":\"result_too_large\"}"),
                Step(504, "{\"code\":\"query_timeout\"}"),
                Step(507, "{\"code\":\"out_of_memory\"}"),
            )
        for (first in listOf(Step(504, outcomeUnknown), Step(503, "ambiguous gateway"))) {
            for (refusal in refusals) {
                servedBodies.clear()
                val (server, calls) = serve(first, Step(503, "{\"code\":\"not_applied\"}"), refusal)
                val c = client("http://127.0.0.1:${server.address.port}")
                val error = catchThrowable { txFixed(c, "sticky-final") }
                assertThat(error).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                assertThat((error as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo("sticky-final")
                assertThat(calls.get()).isEqualTo(3)
                assertThat(servedBodies.distinct()).hasSize(1)
            }
        }
    }

    @Test
    fun `sticky ambiguity survives owner repinning while known rollback and full success remain distinct`() {
        for (ambiguous in listOf(false, true)) {
            for (success in listOf(false, true)) {
                servedBodies.clear()
                val final =
                    if (success) {
                        Step(
                            200,
                            "{\"dup\":true,\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}," +
                                "{\"cols\":[\"snapshot\",\"schema\"],\"rows\":[[7,3]],\"affected\":0}]}",
                        )
                    } else {
                        Step(409, "{\"error\":\"expect_failed\",\"guard_index\":2}")
                    }
                val (owner, ownerCalls) = serve(Step(503, "{\"code\":\"not_applied\"}"), final)
                val first = if (ambiguous) Step(504, outcomeUnknown) else Step(503, "{\"code\":\"not_applied\"}")
                val (entry, entryCalls) =
                    serve(
                        first,
                        Step(421, "{\"error\":\"misdirected\",\"owner\":\"${owner.address.port}\"}"),
                    )
                val c = client("http://127.0.0.1:${entry.address.port}", ownerUrlPattern = "http://127.0.0.1:{owner}")
                val result =
                    runCatching {
                        c.tx(
                            "t",
                            "sticky-owner",
                            "sticky-fingerprint",
                            listOf(HogliteStatement("UPDATE kv SET v=1"), HogliteStatement("SELECT 7,3")),
                        )
                    }
                if (success) {
                    assertThat(result.exceptionOrNull()).isNull()
                    assertThat(result.getOrThrow()).hasSize(2)
                    assertThat(result.getOrThrow()[0].affected).isEqualTo(1)
                    assertThat(result.getOrThrow()[1].rows).containsExactly(listOf(7, 3))
                } else if (ambiguous) {
                    assertThat(
                        result.exceptionOrNull(),
                    ).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                    assertThat(
                        (result.exceptionOrNull() as HoglakeException.CommitOutcomeUnknown).txid,
                    ).isEqualTo("sticky-owner")
                } else {
                    assertThat(result.exceptionOrNull()).isInstanceOf(HogliteGuardFailure::class.java)
                }
                assertThat(entryCalls.get()).isEqualTo(2)
                assertThat(ownerCalls.get()).isEqualTo(2)
                assertThat(servedBodies).hasSize(4)
                assertThat(servedBodies.distinct()).hasSize(1)
                assertThat(servedBodies.first()).contains("\"txid\":\"sticky-owner\"", "\"fp\":\"sticky-fingerprint\"")
            }
        }
    }

    @Test
    fun `sticky write classification does not change query or read transaction refusals`() {
        val (query, _) = serve(Step(504, outcomeUnknown), Step(409, "{\"code\":\"refused\"}"))
        val q = catchThrowable { client("http://127.0.0.1:${query.address.port}").query("t", "SELECT 1") }
        assertThat(q).isInstanceOf(HogliteRejected::class.java)
        assertThat((q as HogliteRejected).status).isEqualTo(409)
        val (batch, _) = serve(Step(504, outcomeUnknown), Step(422, "{\"code\":\"refused\"}"))
        val r =
            catchThrowable {
                client(
                    "http://127.0.0.1:${batch.address.port}",
                ).readTx("t", listOf(HogliteStatement("SELECT 1")))
            }
        assertThat(r).isInstanceOf(HogliteRejected::class.java)
        assertThat((r as HogliteRejected).status).isEqualTo(409)
    }

    @Test
    fun `a budget spent on not_applied 503s is unavailable but not unknown`() {
        val (server, _) = serve(Step(503, "{\"error\":\"lease lost\",\"code\":\"not_applied\"}"))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 300)
        assertThat(catchThrowable { txFixed(c, "tx-na") })
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
    }

    @Test
    fun `no retry is squeezed into the budget's last milliseconds`() {
        // A slow not_applied 503 and a backoff that lands the loop ~25ms
        // before the deadline: a retry started there could only time out,
        // and a tx attempt that timed out after being sent is unknown.
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { ex ->
            ex.requestBody.readBytes()
            Thread.sleep(100)
            val body = "{\"error\":\"lease lost\",\"code\":\"not_applied\"}".toByteArray()
            ex.sendResponseHeaders(503, body.size.toLong())
            ex.responseBody.use { it.write(body) }
        }
        server.start()
        servers += server
        val budgetMs = 1_000L
        var start = 0L
        val c =
            HogliteClient(
                standardUrl = "http://127.0.0.1:${server.address.port}",
                retryBudgetMs = budgetMs,
                sleeper = { Thread.sleep(maxOf(0, start + budgetMs - 25 - System.currentTimeMillis())) },
            )
        start = System.currentTimeMillis()
        // MUTATION: drop the MIN_RETRY_ATTEMPT_MS floor and the last
        // attempt times out after being sent, so this is unknown.
        assertThat(catchThrowable { txFixed(c, "tx-floor") })
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
    }

    @Test
    fun `an unlabelled 503 on a tx may have applied`() {
        // an engine that predates the labels answered 503 for segment and
        // checkpoint failures after apply
        val (server, _) = serve(Step(503, "{\"error\":\"segment put: SlowDown\"}"))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 300)
        assertThatThrownBy { txFixed(c, "tx-old503") }.isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
    }

    @Test
    fun `an unlabelled 422 on a tx is unknown on first sight, not a refusal`() {
        val (server, calls) = serve(Step(422, "{\"error\":\"pod lease write deadline passed\"}"))
        val c = client("http://127.0.0.1:${server.address.port}")
        assertThatThrownBy { txFixed(c, "tx-old422") }
            .isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            .isNotInstanceOf(HogliteRejected::class.java)
        assertThat(calls.get()).isEqualTo(1)
    }

    @Test
    fun `a 500 or 502 on a tx is unknown, not a refusal`() {
        for (status in listOf(500, 502)) {
            val (server, _) = serve(Step(status, "bad gateway"))
            val c = client("http://127.0.0.1:${server.address.port}")
            assertThatThrownBy {
                txFixed(
                    c,
                    "tx-$status",
                )
            }.isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
        }
    }

    @Test
    fun `a refused connection never sent the tx, so exhaustion is not unknown`() {
        val (server, _) = serve(Step(200, "{\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}]}"))
        val port = server.address.port
        server.stop(0)
        val c = client("http://127.0.0.1:$port", budgetMs = 300)
        assertThat(catchThrowable { txFixed(c, "tx-refused") })
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
    }

    @Test
    fun `a read timeout after the tx was sent is unknown`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { ex ->
            ex.requestBody.readBytes()
            Thread.sleep(1_000)
            ex.sendResponseHeaders(200, 2)
            ex.responseBody.use { it.write(byteArrayOf(123, 125)) }
        }
        server.start()
        servers += server
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 200)
        assertThatThrownBy { txFixed(c, "tx-timeout") }.isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
    }

    @Test
    fun `a labelled query guard refusal on a tx is final, whatever its status`() {
        // hoglite's guard refusals rolled back and wrote no receipt; 504
        // query_timeout shares its status with outcome_unknown, so only
        // the label separates "retry this" from "this will never fit".
        for ((status, code) in listOf(504 to "query_timeout", 413 to "result_too_large", 507 to "out_of_memory")) {
            val (server, calls) = serve(Step(status, "{\"error\":\"guard\",\"code\":\"$code\"}"))
            val c = client("http://127.0.0.1:${server.address.port}")
            assertThat(catchThrowable { txFixed(c, "tx-$code") })
                .describedAs(code)
                .isInstanceOf(HogliteRejected::class.java)
                .isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            assertThat(calls.get()).describedAs(code).isEqualTo(1)
        }
    }

    @Test
    fun `a query_timeout on a query is final, and a preempted read retries`() {
        val (server, calls) = serve(Step(504, "{\"error\":\"query timeout\",\"code\":\"query_timeout\"}"))
        val c = client("http://127.0.0.1:${server.address.port}")
        assertThatThrownBy { c.query("t", "SELECT 1") }.isInstanceOf(HogliteRejected::class.java)
        assertThat(calls.get()).isEqualTo(1)
        // upstream's ErrReadPreempted: an unlabelled 503, retried
        val (preempted, calls2) =
            serve(
                Step(503, "{\"error\":\"read canceled by checkpoint; retry\"}"),
                Step(200, "{\"cols\":[\"x\"],\"rows\":[[1]],\"affected\":1}"),
            )
        val c2 = client("http://127.0.0.1:${preempted.address.port}")
        assertThat(c2.query("t", "SELECT 1").rows.single().single()).isEqualTo(1)
        assertThat(calls2.get()).isEqualTo(2)
    }

    @Test
    fun `a query is never unknown, and its 503 and 504 retry`() {
        val (server, calls) =
            serve(
                Step(503, "{\"error\":\"context canceled\",\"code\":\"not_applied\"}"),
                Step(504, "gateway timeout"),
                Step(200, "{\"cols\":[\"x\"],\"rows\":[[1]],\"affected\":1}"),
            )
        val c = client("http://127.0.0.1:${server.address.port}")
        assertThat(c.query("t", "SELECT 1").rows.single().single()).isEqualTo(1)
        assertThat(calls.get()).isEqualTo(3)
        val (down, _) = serve(Step(504, "gateway timeout"))
        val c2 = client("http://127.0.0.1:${down.address.port}", budgetMs = 300)
        assertThat(catchThrowable { c2.query("t", "SELECT 1") })
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
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
        val (server, calls) = serve(Step(200, "{\"results\":[{\"cols\":null,\"rows\":null,\"affected\":1}]}"))
        val c = client("http://127.0.0.1:${server.address.port}", budgetMs = 1)
        Thread.sleep(5) // let wall time pass; the deadline starts inside the call
        assertThatThrownBy { txOne(c) }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(calls.get()).isLessThan(50)
    }

    // ---- read-only transactions (a commit's batched planning reads) ------

    /**
     * Each case is one engine failure, as `/query` and as a read-only `/tx`
     * answer it. The batch must reject exactly as the single read it
     * replaces, so a caller's mapping of [HogliteRejected] (and retry
     * exhaustion) is unchanged: same class, same status.
     */
    @Test
    fun `a failed read transaction rejects exactly as the failed query it replaces`() {
        fun err(code: String) = "{\"error\":\"x\",\"code\":\"$code\"}"
        val cases =
            listOf(
                // A statement error: /query answers 409, a read-only /tx 422, both `refused`.
                Step(409, err("refused")) to Step(422, err("refused")),
                // Query guard refusals: final, same status on both endpoints.
                Step(504, err("query_timeout")) to Step(504, err("query_timeout")),
                Step(413, err("result_too_large")) to Step(413, err("result_too_large")),
                Step(507, err("out_of_memory")) to Step(507, err("out_of_memory")),
                // The tenant is gone; a malformed request; an unlabelled 422.
                Step(404, "{}") to Step(404, "{}"),
                Step(400, "{}") to Step(400, "{}"),
                Step(422, "{}") to Step(422, "{}"),
                // Transient until the budget runs out: unavailable, never "outcome unknown".
                Step(503, "{}") to Step(503, "{}"),
                Step(504, "{}") to Step(504, "{}"),
            )

        fun shape(t: Throwable?) = t?.javaClass to (t as? HogliteRejected)?.status
        for ((asQuery, asTx) in cases) {
            val (qs, _) = serve(asQuery)
            val (ts, _) = serve(asTx)
            val single =
                catchThrowable { client("http://127.0.0.1:${qs.address.port}", budgetMs = 50).query("t", "SELECT 1") }
            val batch =
                catchThrowable {
                    client("http://127.0.0.1:${ts.address.port}", budgetMs = 50)
                        .readTx("t", listOf(HogliteStatement("SELECT 1", emptyList())))
                }
            assertThat(single).describedAs("query ${asQuery.status} ${asQuery.body}").isNotNull()
            assertThat(shape(batch)).describedAs("read tx ${asTx.status} ${asTx.body}").isEqualTo(shape(single))
            assertThat(batch).isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
        }
    }

    @Test
    fun `a read transaction carries plain SELECTs under a read txid and returns one result per statement`() {
        val (server, calls) =
            serve(
                Step(
                    200,
                    "{\"results\":[{\"cols\":[\"x\"],\"rows\":[[1]],\"affected\":1}," +
                        "{\"cols\":[\"y\"],\"rows\":[],\"affected\":0}]}",
                ),
            )
        val c = client("http://127.0.0.1:${server.address.port}")
        val r =
            c.readTx(
                "t",
                listOf(HogliteStatement("SELECT 1", emptyList()), HogliteStatement("SELECT 2 WHERE 0", emptyList())),
            )
        assertThat(r.map { it.rows }).containsExactly(listOf(listOf(1)), emptyList())
        assertThat(calls.get()).isEqualTo(1)
        assertThat(servedBodies.single()).contains("\"txid\":\"${HogliteClient.READ_TXID_PREFIX}")
        // A write or a guard would leave the engine's reader path: refused before sending.
        assertThatThrownBy { c.readTx("t", listOf(HogliteStatement("UPDATE meta SET head = 1", emptyList()))) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy {
            c.readTx("t", listOf(HogliteStatement("SELECT 1", emptyList(), expectScalarInt = 1L)))
        }.isInstanceOf(IllegalArgumentException::class.java)
        // A short answer is a broken engine, not a silently missing read.
        val (short, _) = serve(Step(200, "{\"results\":[{\"cols\":[\"x\"],\"rows\":[[1]],\"affected\":1}]}"))
        assertThatThrownBy {
            client("http://127.0.0.1:${short.address.port}").readTx(
                "t",
                listOf(HogliteStatement("SELECT 1", emptyList()), HogliteStatement("SELECT 2", emptyList())),
            )
        }.isInstanceOf(IllegalStateException::class.java)
        assertThat(calls.get()).isEqualTo(1)
    }
}
