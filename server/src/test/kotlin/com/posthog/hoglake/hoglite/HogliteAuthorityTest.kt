package com.posthog.hoglake.hoglite

import com.posthog.hoglake.auth.AuthState
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.VerifiedBackendAuthority
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
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
                VerifiedBackendAuthority("synthetic-secret"),
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
