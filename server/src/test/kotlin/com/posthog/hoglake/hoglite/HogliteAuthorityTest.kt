package com.posthog.hoglake.hoglite

import com.posthog.hoglake.auth.AuthState
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.VerifiedBackendAuthority
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class HogliteAuthorityTest {
    @Test
    fun `concurrent request wrappers never share caller tokens with each other or the base client`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        server.executor = executor
        val entered = CountDownLatch(2)
        val observed = ConcurrentLinkedQueue<Pair<String, String?>>()
        server.createContext("/") { exchange ->
            val tenant = exchange.requestURI.path.split('/')[3]
            val token = exchange.requestHeaders.getFirst("Authorization")
            if (tenant != "base") {
                entered.countDown()
                check(entered.await(10, TimeUnit.SECONDS))
            }
            observed.add(tenant to token)
            val bytes = """{"cols":["x"],"rows":[[1]],"affected":0}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        server.start()
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}").use { shared ->
                val first = shared.forRequest("synthetic-first")
                val second = shared.forRequest("synthetic-second")
                val one = executor.submit { first.query("one", "SELECT 1") }
                val two = executor.submit { second.query("two", "SELECT 1") }
                one.get(15, TimeUnit.SECONDS)
                two.get(15, TimeUnit.SECONDS)
                shared.query("base", "SELECT 1")
                assertThat(observed).containsExactlyInAnyOrder(
                    "one" to "Bearer synthetic-first",
                    "two" to "Bearer synthetic-second",
                    "base" to null,
                )
            }
        } finally {
            server.stop(0)
            executor.close()
        }
    }

    @Test
    fun `expired authority after an uncertain write never proves the write did not happen`() {
        val calls = AtomicInteger()
        val observed = ConcurrentLinkedQueue<String?>()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            exchange.requestBody.readBytes()
            observed.add(exchange.requestHeaders.getFirst("Authorization"))
            if (calls.incrementAndGet() == 1) {
                val body = """{"code":"outcome_unknown","error":"lost durable acknowledgement"}""".toByteArray()
                exchange.sendResponseHeaders(504, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            } else {
                val body = """{"code":"refused","error":"request not authorized"}""".toByteArray()
                exchange.sendResponseHeaders(401, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
        }
        server.start()
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                assertThatThrownBy {
                    shared.forRequest("synthetic-expiring").tx(
                        "one",
                        "same-txid",
                        null,
                        listOf(HogliteStatement("INSERT INTO x VALUES(1)", emptyList())),
                    )
                }.isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                assertThat(calls.get()).isEqualTo(2)
                assertThat(observed).containsOnly("Bearer synthetic-expiring")
            }
        } finally {
            server.stop(0)
        }
    }

    private val applied = 200 to """{"results":[{"cols":[],"rows":[],"affected":1}]}"""

    /**
     * An engine whose first `/tx` answers [first] (a one-statement write
     * result by default) and whose every later call answers [status], as a
     * token that expired between hops would.
     */
    private fun refusingAfterFirstTx(
        status: Int,
        first: Pair<Int, String> = applied,
    ): HttpServer = refusingAfterTxs(status, first)

    /** An engine whose `/tx` attempts answer [txs] in order, and every other call [status]. */
    private fun refusingAfterTxs(
        status: Int,
        vararg txs: Pair<Int, String>,
    ): HttpServer {
        val attempts = AtomicInteger()
        return HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { exchange ->
                exchange.requestBody.readBytes()
                val (code, text) =
                    if (exchange.requestURI.path.endsWith("/tx")) {
                        txs.getOrNull(attempts.getAndIncrement())
                    } else {
                        null
                    } ?: (status to """{"code":"refused","error":"request not authorized"}""")
                val body = text.toByteArray()
                exchange.sendResponseHeaders(code, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            start()
        }
    }

    private val write = listOf(HogliteStatement("INSERT INTO x VALUES(1)", emptyList()))

    private val hops: List<Pair<String, (HogliteClient) -> Unit>> =
        listOf(
            "query" to { c -> c.query("one", "SELECT 1") },
            "read" to { c -> c.readTx("one", listOf(HogliteStatement("SELECT 1"))) },
            "tx" to { c -> c.tx("one", "second", null, write) },
            "release" to { c -> c.release("one") },
            "delete" to { c -> c.deleteTenant("one") },
        )

    @Test
    fun `a refused token after an applied transaction of the request never reads as nothing ran`() {
        for ((name, hop) in hops) {
            val server = refusingAfterFirstTx(401)
            try {
                HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                    val selected = shared.forRequest("synthetic-expiring")
                    selected.tx("one", "first", null, write)
                    val failure = catchThrowable { hop(selected) }
                    assertThat(failure).describedAs(name).isExactlyInstanceOf(
                        HoglakeException.CommitOutcomeUnknown::class.java,
                    )
                    val unknown = failure as HoglakeException.CommitOutcomeUnknown
                    assertThat(unknown.txid).isEqualTo("first")
                    // "first" answered 200: the outcome is known, the request partly applied.
                    assertThat(unknown.applied).describedAs(name).isTrue()
                    assertThat(unknown.message).describedAs(name)
                        .contains("request partly applied", "'first'").doesNotContain("may or may not")
                    // No promise of exactly-once to a route that takes no idempotency_key.
                    assertThat(unknown.message).describedAs(name)
                        .contains("Only an operation that takes an idempotency_key")
                    // Another request, with nothing applied, keeps the refusal it can re-mint for.
                    assertThat(catchThrowable { hop(shared.forRequest("synthetic-expiring")) })
                        .describedAs(name).isExactlyInstanceOf(HogliteRejected::class.java)
                }
            } finally {
                server.stop(0)
            }
        }
    }

    /** hoglite answers 403 only for authority that never covers the call, which no retry changes. */
    @Test
    fun `a forbidden call after an applied transaction of the request stays a refusal`() {
        for ((name, hop) in hops) {
            val server = refusingAfterFirstTx(403)
            try {
                HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                    val selected = shared.forRequest("synthetic-forbidden")
                    selected.tx("one", "first", null, write)
                    val failure = catchThrowable { hop(selected) }
                    assertThat(failure).describedAs(name).isExactlyInstanceOf(HogliteRejected::class.java)
                    assertThat((failure as HogliteRejected).status).describedAs(name).isEqualTo(403)
                }
            } finally {
                server.stop(0)
            }
        }
    }

    @Test
    fun `a refused authority after an uncertain transaction names that transaction`() {
        val server = refusingAfterFirstTx(401, 500 to """{"error":"proxy failure"}""")
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                val selected = shared.forRequest("synthetic-expiring")
                val own = catchThrowable { selected.tx("one", "uncertain", null, write) }
                assertThat(own).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                assertThat((own as HoglakeException.CommitOutcomeUnknown).tokenLapsed).isFalse()
                // A refused tx never ran: its 401 names "uncertain", and "refused" is recorded nowhere.
                assertThatThrownBy { selected.tx("one", "refused", null, write) }
                    .isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                val failure = catchThrowable { selected.query("one", "SELECT 1") }
                assertThat(failure).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                val unknown = failure as HoglakeException.CommitOutcomeUnknown
                assertThat(unknown.txid).isEqualTo("uncertain")
                assertThat(unknown.applied).isFalse()
                assertThat(unknown.tokenLapsed).isTrue()
                assertThat(unknown.message).contains("may or may not", "'uncertain'")
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `a refused tx after an applied one keeps the request partly applied`() {
        val server = refusingAfterTxs(401, applied)
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                val selected = shared.forRequest("synthetic-expiring")
                selected.tx("one", "first", null, write)
                // A 401 naming "first" is not "second"'s own unknown outcome.
                val refusedTx = catchThrowable { selected.tx("one", "second", null, write) }
                assertThat((refusedTx as HoglakeException.CommitOutcomeUnknown).applied).isTrue()
                val failure = catchThrowable { selected.query("one", "SELECT 1") }
                assertThat(failure).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                val unknown = failure as HoglakeException.CommitOutcomeUnknown
                assertThat(unknown.txid).isEqualTo("first")
                assertThat(unknown.applied).isTrue()
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `an uncertain tx after an applied one never downgrades the request to an unknown outcome`() {
        // "second" answers 500, then its retry the 401 every later call gets.
        val server = refusingAfterTxs(401, applied, 500 to """{"error":"proxy failure"}""")
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                val selected = shared.forRequest("synthetic-expiring")
                selected.tx("one", "first", null, write)
                val uncertain = catchThrowable { selected.tx("one", "second", null, write) }
                assertThat((uncertain as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo("second")
                assertThat(uncertain.applied).isFalse()
                // Its own unknown outcome, and no 401 ended it: a best-effort step may count it.
                assertThat(uncertain.tokenLapsed).isFalse()
                val failure = catchThrowable { selected.query("one", "SELECT 1") }
                assertThat(failure).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                val unknown = failure as HoglakeException.CommitOutcomeUnknown
                assertThat(unknown.txid).isEqualTo("first")
                assertThat(unknown.applied).isTrue()
                assertThat(unknown.tokenLapsed).isTrue()
            }
        } finally {
            server.stop(0)
        }
    }

    /** A 504 and then a 401 for the retry: the tx's own outcome is unknown, and the token lapsed. */
    @Test
    fun `a 401 ending an ambiguous tx marks the lapse only after an earlier tx of the request`() {
        val gatewayTimeout = 504 to """{"error":"gateway timeout"}"""
        val refused = 401 to """{"code":"refused","error":"request not authorized"}"""
        // "first" before "second": none, one that may have applied (504, then 401), one that did.
        for (earlier in listOf("none", "uncertain", "applied")) {
            val txs =
                when (earlier) {
                    "none" -> arrayOf(gatewayTimeout)
                    "uncertain" -> arrayOf(gatewayTimeout, refused, gatewayTimeout)
                    else -> arrayOf(applied, gatewayTimeout)
                }
            val server = refusingAfterTxs(401, *txs)
            try {
                HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                    val selected = shared.forRequest("synthetic-expiring")
                    if (earlier != "none") runCatching { selected.tx("one", "first", null, write) }
                    val failure = catchThrowable { selected.tx("one", "second", null, write) }
                    assertThat(failure).describedAs(earlier)
                        .isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                    val unknown = failure as HoglakeException.CommitOutcomeUnknown
                    // Only a request that partly ran fails a best-effort maintenance run on it.
                    assertThat(unknown.tokenLapsed).describedAs(earlier).isEqualTo(earlier != "none")
                    if (earlier == "applied") {
                        // The request certainly partly applied: never "may or may not".
                        assertThat(unknown.txid).isEqualTo("first")
                        assertThat(unknown.applied).isTrue()
                        assertThat(unknown.message)
                            .contains("request partly applied", "'first'", "'second'").doesNotContain("may or may not")
                    } else {
                        assertThat(unknown.txid).describedAs(earlier).isEqualTo("second")
                        assertThat(unknown.applied).describedAs(earlier).isFalse()
                    }
                }
            } finally {
                server.stop(0)
            }
        }
    }

    @Test
    fun `a refused authority after a transaction that did not apply stays a refusal`() {
        val guard = 409 to """{"code":"expect_failed","guard_index":1,"error":"expect_failed at statement 0"}"""
        val server = refusingAfterFirstTx(401, guard)
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                val selected = shared.forRequest("synthetic-expiring")
                assertThatThrownBy { selected.tx("one", "guarded", null, write) }
                    .isInstanceOf(HogliteGuardFailure::class.java)
                assertThat(catchThrowable { selected.query("one", "SELECT 1") })
                    .isExactlyInstanceOf(HogliteRejected::class.java)
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `the shared client serves many requests and never carries one's applied transaction to another`() {
        val server = refusingAfterFirstTx(401)
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                shared.tx("one", "first", null, write)
                assertThat(catchThrowable { shared.query("one", "SELECT 1") })
                    .isExactlyInstanceOf(HogliteRejected::class.java)
            }
        } finally {
            server.stop(0)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [401, 403])
    fun `the store keeps a refused token after an applied transaction an unknown outcome, a forbidden call forbidden`(
        status: Int,
    ) {
        val server = refusingAfterFirstTx(status)
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { shared ->
                val store = HogliteCatalogStore(shared.forRequest("synthetic-expiring"), { "opaque_1" })
                val tx = HogliteTx().apply { add("INSERT INTO x VALUES(1)") }
                val failure =
                    catchThrowable {
                        store.catalogOperation("cat") {
                            store.runTx("cat", "applied", null, tx, fenced = false)
                            store.queryCatalog("cat", "SELECT 1")
                        }
                    }
                if (status == 403) {
                    assertThat(failure).isExactlyInstanceOf(HoglakeException.Forbidden::class.java)
                } else {
                    assertThat(failure).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
                    assertThat((failure as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo("applied")
                    assertThat(failure.applied).isTrue()
                }
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `authenticated request state never renders its bearer credential`() {
        val state =
            AuthState.Authenticated(
                Caller(
                    subject = "test",
                    orgId = "org",
                    kind = Caller.Kind.TENANT,
                    catalogScopes = mapOf("lake" to setOf(Scope.READ)),
                    tokenId = "jti",
                    clientName = null,
                ),
                VerifiedBackendAuthority("synthetic-secret", verifiedAt = 0),
            )
        assertThat(state.toString()).doesNotContain("synthetic-secret")
    }

    @Test
    fun `invalid authenticated owner hints never change routing or enter diagnostics`() {
        val hints =
            listOf(
                "../escape", "evil/route", "user@host", "host:80", "host?x=1", "host#frag",
                "-host", "host-", "a".repeat(64), "pod.bad/nonce", "pod.bad.extra", "pod\r\nheader",
            )
        for (hint in hints) {
            val calls = AtomicInteger()
            val paths = ConcurrentLinkedQueue<String>()
            val json = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            server.createContext("/") { ex ->
                paths.add(ex.requestURI.path)
                ex.requestBody.readBytes()
                val first = calls.incrementAndGet() == 1
                val body =
                    if (first) {
                        json.writeValueAsBytes(mapOf("owner" to hint))
                    } else {
                        """{"cols":["x"],"rows":[[1]],"affected":0}""".toByteArray()
                    }
                ex.sendResponseHeaders(if (first) 421 else 200, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            server.start()
            try {
                val url = "http://127.0.0.1:${server.address.port}"
                HogliteClient(url, ownerUrlPattern = "$url/owner/{owner}", sleeper = {}).use { shared ->
                    shared.forRequest("synthetic-owner").query("one", "SELECT 1")
                    assertThat(paths).containsOnly("/v1/t/one/query")
                    assertThat(calls.get()).isEqualTo(2)
                }
            } finally {
                server.stop(0)
            }
        }
    }

    @Test
    fun `exhausted owner handoff retries never echo an untrusted response body`() {
        val calls = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { ex ->
            ex.requestBody.readBytes()
            val warmup = calls.incrementAndGet() == 1
            val body =
                if (warmup) {
                    """{"cols":["x"],"rows":[[1]],"affected":0}"""
                } else {
                    """{"owner":"secret-owner/escape"}"""
                }.toByteArray()
            ex.sendResponseHeaders(if (warmup) 200 else 421, body.size.toLong())
            ex.responseBody.use { it.write(body) }
        }
        server.start()
        try {
            val url = "http://127.0.0.1:${server.address.port}"
            HogliteClient(url, ownerUrlPattern = "$url/owner/{owner}", retryBudgetMs = 1_000, sleeper = {}).use {
                    shared ->
                val selected = shared.forRequest("synthetic-owner")
                selected.query("one", "SELECT 1")
                val failure = runCatching { selected.query("one", "SELECT 1") }.exceptionOrNull()
                assertThat(failure).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                assertThat(failure?.message).contains("421").doesNotContain("secret-owner")
                assertThat(calls.get()).isGreaterThan(1)
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `invalid owner hints evict stale pins and preserve whale priority`() {
        for (whale in listOf(false, true)) {
            val calls = AtomicInteger()
            val paths = ConcurrentLinkedQueue<String>()
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            server.createContext("/") { ex ->
                ex.requestBody.readBytes()
                paths.add(ex.requestURI.path)
                val attempt = calls.incrementAndGet()
                val body =
                    when (attempt) {
                        1 -> """{"owner":"old.boot"}"""
                        3 -> if (whale) """{"owner":"bad/escape","class":"whale"}""" else """{"owner":"bad/escape"}"""
                        else -> """{"cols":["x"],"rows":[[1]],"affected":0}"""
                    }.toByteArray()
                ex.sendResponseHeaders(if (attempt == 1 || attempt == 3) 421 else 200, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            server.start()
            try {
                val url = "http://127.0.0.1:${server.address.port}"
                HogliteClient(
                    "$url/standard",
                    whaleUrl = "$url/whale",
                    ownerUrlPattern = "$url/owner/{owner}",
                    sleeper = {},
                ).use { shared ->
                    val selected = shared.forRequest("synthetic-owner")
                    selected.query("one", "SELECT 1")
                    selected.query("one", "SELECT 1")
                    assertThat(paths).containsExactly(
                        "/standard/v1/t/one/query",
                        "/owner/old/v1/t/one/query",
                        "/owner/old/v1/t/one/query",
                        if (whale) "/whale/v1/t/one/query" else "/standard/v1/t/one/query",
                    )
                }
            } finally {
                server.stop(0)
            }
        }
    }
}
