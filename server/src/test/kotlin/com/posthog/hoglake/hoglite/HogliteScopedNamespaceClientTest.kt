package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/** These fixtures exercise only the outbound protocol, not JWT verification or route authorization. */
class HogliteScopedNamespaceClientTest {
    private val servers = mutableListOf<HttpServer>()
    private val executor = Executors.newVirtualThreadPerTaskExecutor()
    private val json = jacksonObjectMapper()

    @AfterEach
    fun stop() {
        servers.forEach { it.stop(0) }
        executor.shutdownNow()
    }

    private fun serve(handler: (HttpExchange) -> Unit): HttpServer {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = executor
        server.createContext("/") { exchange ->
            exchange.use { handler(it) }
        }
        server.start()
        servers += server
        return server
    }

    private fun url(server: HttpServer) = "http://127.0.0.1:${server.address.port}"

    private fun reply(
        exchange: HttpExchange,
        body: ByteArray,
        status: Int = 200,
        chunked: Boolean = false,
    ) {
        exchange.responseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(status, if (chunked) 0 else body.size.toLong())
        exchange.responseBody.write(body)
    }

    private fun validBody(namespace: String = "reports"): String =
        """{"namespace_id":17,"name":${json.writeValueAsString(
            namespace,
        )},"containment":{"field":"account_id","value":42}}"""

    private fun expectUnavailable(
        client: HogliteClient,
        tenant: String = "tenant",
        catalog: String = "catalog",
        namespace: String = "reports",
    ) {
        assertThatThrownBy { client.readScopedNamespace(tenant, catalog, namespace) }
            .isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessage("hoglite scoped namespace read unavailable")
            .hasNoCause()
    }

    @Test
    fun `the typed GET preserves the token and canonically encodes each segment`() {
        val calls = AtomicInteger()
        val namespace = "reports +?雪😀"
        val server =
            serve { exchange ->
                calls.incrementAndGet()
                assertThat(exchange.requestMethod).isEqualTo("GET")
                assertThat(exchange.requestURI.rawPath).isEqualTo(
                    "/v1/t/tenant_1/metadata/v1/catalogs/lake/namespaces/reports%20%2B%3F%E9%9B%AA%F0%9F%98%80",
                )
                assertThat(exchange.requestURI.rawQuery).isNull()
                assertThat(exchange.requestBody.readBytes()).isEmpty()
                assertThat(exchange.requestHeaders.getFirst("Authorization")).isEqualTo("Bearer $TOKEN")
                assertThat(exchange.requestHeaders.getFirst("Accept")).isEqualTo("application/json")
                assertThat(exchange.requestHeaders.getFirst("x-hw-noproxy")).isNull()
                reply(exchange, validBody(namespace).toByteArray())
            }
        HogliteClient(url(server)).use { base ->
            val result = base.forRequest(TOKEN).readScopedNamespace("tenant_1", "lake", namespace)!!
            assertThat(result.namespaceId).isEqualTo(17L)
            assertThat(result.name).isEqualTo(namespace)
            assertThat(result.containment?.field).isEqualTo("account_id")
            assertThat(result.containment?.value).isEqualTo(42L)
            assertThat(result.containment?.org).isNull()
            assertThat(result.remote).isNull()
        }
        assertThat(calls.get()).isEqualTo(1)
    }

    @Test
    fun `a strict success accepts the positive signed 64-bit boundary`() {
        val body = validBody().replace(":17", ":9223372036854775807").replace(":42", ":9223372036854775807")
        val server = serve { reply(it, body.toByteArray()) }
        HogliteClient(url(server)).use { base ->
            val result = base.forRequest(TOKEN).readScopedNamespace("tenant", "catalog", "reports")!!
            assertThat(result.namespaceId).isEqualTo(Long.MAX_VALUE)
            assertThat(result.containment?.value).isEqualTo(Long.MAX_VALUE)
        }
    }

    @Test
    fun `only the exact not_found object means absence`() {
        val bodies =
            listOf(
                """{"code":"not_found"}""",
                """  {"code":"not_found"}  """,
                """{"code":"unavailable"}""",
                """{"code":"not_found","owner":"private-peer"}""",
                """{"code":"not_found","code":"not_found"}""",
                """{"code":"not_found"}{}""",
                """{"code":null}""",
                """{"error":"not_found"}""",
                "null",
                "[]",
                "",
            )
        for ((index, body) in bodies.withIndex()) {
            val calls = AtomicInteger()
            val server =
                serve {
                    calls.incrementAndGet()
                    reply(it, body.toByteArray(), 404, chunked = body.isEmpty())
                }
            HogliteClient(url(server)).use { base ->
                val client = base.forRequest(TOKEN)
                if (index < 2) {
                    assertThat(client.readScopedNamespace("tenant", "catalog", "reports")).isNull()
                } else {
                    expectUnavailable(client)
                }
            }
            assertThat(calls.get()).isEqualTo(1)
        }
    }

    @Test
    fun `unexpected statuses never disclose bodies retry or follow redirects`() {
        val redirectedCalls = AtomicInteger()
        val redirected =
            serve {
                redirectedCalls.incrementAndGet()
                reply(it, validBody().toByteArray())
            }
        for (status in listOf(201, 301, 302, 307, 308, 400, 401, 403, 405, 410, 421, 429, 500, 503, 504)) {
            val calls = AtomicInteger()
            val server =
                serve {
                    calls.incrementAndGet()
                    it.responseHeaders.set("Location", url(redirected) + "/private-owner")
                    reply(it, """{"code":"not_found","detail":"$TOKEN private-peer"}""".toByteArray(), status)
                }
            HogliteClient(url(server), retryBudgetMs = 50, sleeper = { error("unexpected retry") }).use { base ->
                expectUnavailable(base.forRequest(TOKEN))
            }
            assertThat(calls.get()).isEqualTo(1)
        }
        assertThat(redirectedCalls.get()).isZero()
    }

    @Test
    fun `success requires exactly the fixed DTO fields and types`() {
        val good = validBody()
        val bodies =
            listOf(
                "{}",
                "null",
                "[]",
                "[$good]",
                "$good $good",
                good.dropLast(1),
                good.replace("\"namespace_id\":17", "\"namespace_id\":17,\"namespace_id\":17"),
                good.replace("\"field\":\"account_id\"", "\"field\":\"account_id\",\"field\":\"account_id\""),
                good.replace("\"namespace_id\":17,", ""),
                good.replace("\"name\":\"reports\",", ""),
                good.replace(",\"containment\":{\"field\":\"account_id\",\"value\":42}", ""),
                good.replace("\"name\":\"reports\"", "\"name\":\"other\""),
                good.replace("\"name\":\"reports\"", "\"name\":42"),
                good.replace("\"name\":\"reports\"", "\"name\":null"),
                good.replace("\"containment\":{\"field\":\"account_id\",\"value\":42}", "\"containment\":null"),
                good.replace("\"field\":\"account_id\",", ""),
                good.replace("\"field\":\"account_id\"", "\"field\":42"),
                good.replace("\"field\":\"account_id\"", "\"field\":null"),
                good.replace("\"field\":\"account_id\"", "\"field\":\"\""),
                good.replace(",\"value\":42", ""),
                good.replace("\"value\":42", "\"value\":42,\"org\":null"),
                good.replace("\"value\":42", "\"value\":42,\"extra\":\"private-metadata\""),
                good.dropLast(1) + ""","remote":true}""",
                good.dropLast(1) + ""","extra":"private-metadata"}""",
            ) +
                listOf("\"17\"", "17.0", "1e1", "0", "-1", "true", "null", "9223372036854775808").flatMap {
                    listOf(
                        good.replace("\"namespace_id\":17", "\"namespace_id\":$it"),
                        good.replace("\"value\":42", "\"value\":$it"),
                    )
                }
        val next = AtomicInteger()
        val server = serve { reply(it, bodies[next.getAndIncrement()].toByteArray()) }
        HogliteClient(url(server)).use { base ->
            val client = base.forRequest(TOKEN)
            repeat(bodies.size) { expectUnavailable(client) }
        }
        assertThat(next.get()).isEqualTo(bodies.size)
    }

    @Test
    fun `responses require valid UTF-8 JSON`() {
        val bodies =
            listOf(
                validBody().toByteArray(Charsets.UTF_16),
                byteArrayOf(0xEF.toByte(), 0xBB.toByte(), 0xBF.toByte()) + validBody().toByteArray(),
                validBody().replace("reports", "\u0000").toByteArray(),
                validBody().toByteArray().let { it.copyOf().apply { this[0] = 0xFF.toByte() } },
                validBody().replace("\"value\":42", "\"value\":[[42]]").toByteArray(),
            )
        val next = AtomicInteger()
        val server = serve { reply(it, bodies[next.getAndIncrement()]) }
        HogliteClient(url(server)).use { base ->
            repeat(bodies.size) { expectUnavailable(base.forRequest(TOKEN)) }
        }
    }

    @Test
    fun `the response byte ceiling applies to declared and chunked bodies before parsing`() {
        val good = validBody()
        for (chunked in listOf(false, true)) {
            for (length in listOf(4096, 4097)) {
                val body = good.padEnd(length, ' ').toByteArray()
                val server = serve { reply(it, body, chunked = chunked) }
                HogliteClient(url(server)).use { base ->
                    val client = base.forRequest(TOKEN)
                    if (length == 4096) {
                        assertThat(
                            client.readScopedNamespace("tenant", "catalog", "reports")?.namespaceId,
                        ).isEqualTo(17L)
                    } else {
                        expectUnavailable(client)
                    }
                }
            }
        }
    }

    @Test
    fun `an oversized declared response fails before the body arrives`() {
        val release = CountDownLatch(1)
        val server =
            serve {
                it.sendResponseHeaders(200, Long.MAX_VALUE)
                it.responseBody.flush()
                release.await(5, TimeUnit.SECONDS)
            }
        try {
            HogliteClient(url(server), readTimeoutMs = 3_000).use { base ->
                val start = System.nanoTime()
                expectUnavailable(base.forRequest(TOKEN))
                assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(2_000L)
            }
        } finally {
            release.countDown()
        }
    }

    @Test
    fun `ambiguous segments and oversized tokens fail without dispatch`() {
        val calls = AtomicInteger()
        val server =
            serve {
                calls.incrementAndGet()
                reply(it, validBody().toByteArray())
            }
        HogliteClient(url(server)).use { base ->
            expectUnavailable(base)
            for (token in listOf("", "not-compact", "$TOKEN\r\nX-Test: value", "a".repeat(65533) + ".b.c")) {
                expectUnavailable(base.forRequest(token))
            }
            expectUnavailable(base.forRequest(null))
            val client = base.forRequest(TOKEN)
            val invalidSegments =
                listOf(
                    "", ".", "..", "a/b", "a\\b", "a%b", "a\u0000", "a\u001F", "a\u007F", "a\u0085", "\uD800", "\uDC00",
                )
            for (segment in invalidSegments) {
                expectUnavailable(client, tenant = segment)
                expectUnavailable(client, catalog = segment)
                expectUnavailable(client, namespace = segment)
            }
            expectUnavailable(client, namespace = "a".repeat(4097))
            expectUnavailable(client, namespace = "雪".repeat(1000))
        }
        assertThat(calls.get()).isZero()
    }

    @Test
    fun `the target and compact token ceilings accept the exact byte boundary`() {
        val prefix = "/v1/t/tenant/metadata/v1/catalogs/catalog/namespaces/"
        val namespace = "n".repeat(4096 - prefix.length)
        val observed = ConcurrentLinkedQueue<String>()
        val server =
            serve {
                observed.add(it.requestHeaders.getFirst("Authorization"))
                assertThat(it.requestURI.rawPath.length).isEqualTo(4096)
                reply(it, """{"code":"not_found"}""".toByteArray(), 404)
            }
        val token = "a".repeat(65532) + ".b.c"
        HogliteClient(url(server)).use { base ->
            assertThat(base.forRequest(token).readScopedNamespace("tenant", "catalog", namespace)).isNull()
            expectUnavailable(base.forRequest(TOKEN), namespace = namespace + "n")
        }
        assertThat(observed).containsExactly("Bearer $token")
    }

    @Test
    fun `the operation ignores owner and whale pins`() {
        for (whale in listOf(false, true)) {
            val paths = ConcurrentLinkedQueue<String>()
            val server =
                serve {
                    val path = it.requestURI.rawPath
                    paths.add(path)
                    when {
                        path == "/standard/v1/t/tenant/query" ->
                            reply(
                                it,
                                (if (whale) """{"class":"whale"}""" else """{"owner":"pod-a"}""").toByteArray(),
                                421,
                            )
                        path.endsWith(
                            "/query",
                        ) -> reply(it, """{"cols":["x"],"rows":[[1]],"affected":0}""".toByteArray())
                        else -> reply(it, validBody().toByteArray())
                    }
                }
            HogliteClient(
                standardUrl = url(server) + "/standard",
                whaleUrl = url(server) + "/whale",
                ownerUrlPattern = url(server) + "/owner/{owner}",
                sleeper = {},
            ).use { base ->
                val client = base.forRequest(TOKEN)
                client.query("tenant", "SELECT 1")
                assertThat(client.readScopedNamespace("tenant", "catalog", "reports")?.namespaceId).isEqualTo(17L)
            }
            assertThat(paths.last()).isEqualTo("/standard/v1/t/tenant/metadata/v1/catalogs/catalog/namespaces/reports")
            assertThat(paths).hasSize(3)
        }
    }

    @Test
    fun `the single budget covers response headers and the entire body`() {
        for (phase in listOf("headers", "declared-body", "chunked-body")) {
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val calls = AtomicInteger()
            val server =
                serve {
                    calls.incrementAndGet()
                    if (phase != "headers") {
                        it.sendResponseHeaders(200, if (phase == "chunked-body") 0 else 4096)
                        it.responseBody.write('{'.code)
                        it.responseBody.flush()
                    }
                    entered.countDown()
                    release.await(5, TimeUnit.SECONDS)
                }
            try {
                HogliteClient(url(server), readTimeoutMs = 150, retryBudgetMs = 10_000).use { base ->
                    val start = System.nanoTime()
                    expectUnavailable(base.forRequest(TOKEN))
                    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(2_000L)
                }
                assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue()
                assertThat(calls.get()).isEqualTo(1)
            } finally {
                release.countDown()
            }
        }
    }

    @Test
    fun `interruption cancels the read and preserves the interrupt flag`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val failure = AtomicReference<Throwable>()
        val interrupted = AtomicBoolean()
        val server =
            serve {
                it.sendResponseHeaders(200, 0)
                it.responseBody.write('{'.code)
                it.responseBody.flush()
                entered.countDown()
                release.await(5, TimeUnit.SECONDS)
            }
        val client = HogliteClient(url(server)).forRequest(TOKEN)
        val caller =
            Thread {
                try {
                    client.readScopedNamespace("tenant", "catalog", "reports")
                } catch (error: Throwable) {
                    failure.set(error)
                    interrupted.set(Thread.currentThread().isInterrupted)
                }
            }
        try {
            caller.start()
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue()
            caller.interrupt()
            caller.join(2_000)
            assertThat(caller.isAlive).isFalse()
            assertThat(failure.get()).isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThat(interrupted.get()).isTrue()
        } finally {
            caller.interrupt()
            release.countDown()
        }
    }

    @Test
    fun `concurrent request wrappers preserve separate original tokens without caching results`() {
        val entered = CountDownLatch(2)
        val observed = ConcurrentLinkedQueue<String>()
        val server =
            serve {
                observed.add(it.requestHeaders.getFirst("Authorization"))
                entered.countDown()
                check(entered.await(2, TimeUnit.SECONDS))
                reply(it, validBody().toByteArray())
            }
        HogliteClient(url(server)).use { base ->
            val first = executor.submit { base.forRequest(TOKEN).readScopedNamespace("tenant", "catalog", "reports") }
            val second =
                executor.submit {
                    base.forRequest(
                        OTHER_TOKEN,
                    ).readScopedNamespace("tenant", "catalog", "reports")
                }
            first.get(3, TimeUnit.SECONDS)
            second.get(3, TimeUnit.SECONDS)
            assertThat(observed).containsExactlyInAnyOrder("Bearer $TOKEN", "Bearer $OTHER_TOKEN")
        }
    }

    @Test
    fun `transport failure remains sanitized unavailable`() {
        val server = serve { reply(it, validBody().toByteArray()) }
        server.stop(0)
        HogliteClient(url(server), connectTimeoutMs = 100, readTimeoutMs = 150).use { base ->
            expectUnavailable(base.forRequest(TOKEN))
        }
    }

    companion object {
        // Synthetic compact values stand in for authority that the caller already verifies.
        private const val TOKEN = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJmaXh0dXJlLW9uZSJ9.c2ln"
        private const val OTHER_TOKEN = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJmaXh0dXJlLXR3byJ9.c2ln"
    }
}
