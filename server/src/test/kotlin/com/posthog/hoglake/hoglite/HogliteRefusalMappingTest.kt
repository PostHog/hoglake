package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

/** Deterministic engine refusals must not invite unchanged retries. */
class HogliteRefusalMappingTest {
    private val store = HogliteCatalogStore(HogliteClient("http://unused.invalid"), { "opaque_1" })

    @ParameterizedTest
    @ValueSource(ints = [400, 409, 422])
    fun `SQL refusals and malformed requests map to Validation including native 422`(status: Int) {
        val error = store.mapRejected("cat", HogliteRejected(status, """{"code":"refused","error":"SQL rejected"}"""))
        assertThat(error).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(error.message).contains("SQL rejected")
        assertThat(error).isNotInstanceOf(HoglakeException.BackendUnavailable::class.java)
    }

    @ParameterizedTest
    @ValueSource(ints = [400, 409, 422])
    fun `known malformed and SQL refusal statuses stay final without a label`(status: Int) {
        assertThat(store.mapRejected("cat", HogliteRejected(status, "malformed request")))
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `result cap and engine query timeout have separate nonretryable types`() {
        assertThat(store.mapRejected("cat", HogliteRejected(413, """{"code":"result_too_large"}""")))
            .isExactlyInstanceOf(HoglakeException.BackendResultTooLarge::class.java)
        assertThat(store.mapRejected("cat", HogliteRejected(504, """{"code":"query_timeout"}""")))
            .isExactlyInstanceOf(HoglakeException.BackendQueryTimeout::class.java)
    }

    @Test
    fun `only transient engine rejection statuses retain outage semantics`() {
        assertThat(store.mapRejected("cat", HogliteRejected(503, """{"code":"not_applied"}""")))
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(store.mapRejected("cat", HogliteRejected(507, """{"code":"out_of_memory"}""")))
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `read transaction refused SQL reaches Validation after one transport attempt`() {
        val calls = AtomicInteger()
        val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { ex ->
                    calls.incrementAndGet()
                    val bytes = """{"code":"refused","error":"invalid SQL"}""".toByteArray()
                    ex.sendResponseHeaders(422, bytes.size.toLong())
                    ex.responseBody.use { it.write(bytes) }
                }
                start()
            }
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}").use { client ->
                val selected = HogliteCatalogStore(client, { "opaque_1" })
                assertThatThrownBy {
                    selected.catalogOperation("cat") {
                        selected.readCatalog("cat", listOf(HogliteStatement("SELECT invalid")))
                    }
                }.isInstanceOf(HoglakeException.Validation::class.java)
            }
            assertThat(calls.get()).isEqualTo(1)
        } finally {
            server.stop(0)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [500, 502])
    fun `unknown server errors remain retryable outages`(status: Int) {
        for (body in listOf("{}", "gateway failure", """{"code":"internal_error","error":"failed"}""")) {
            assertThat(store.mapRejected("cat", HogliteRejected(status, body)))
                .isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [500, 502])
    fun `both read channels preserve unknown server failures`(status: Int) {
        val calls = AtomicInteger()
        val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { ex ->
                    ex.requestBody.readBytes()
                    calls.incrementAndGet()
                    val bytes = """{"error":"upstream failed"}""".toByteArray()
                    ex.sendResponseHeaders(status, bytes.size.toLong())
                    ex.responseBody.use { it.write(bytes) }
                }
                start()
            }
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}").use { client ->
                val selected = HogliteCatalogStore(client, { "opaque_1" })
                for (batch in listOf(false, true)) {
                    assertThatThrownBy {
                        selected.catalogOperation("cat") {
                            if (batch) {
                                selected.readCatalog("cat", listOf(HogliteStatement("SELECT 1")))
                            } else {
                                selected.queryCatalog("cat", "SELECT 1")
                            }
                        }
                    }.isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
                }
            }
            assertThat(calls.get()).isEqualTo(2)
        } finally {
            server.stop(0)
        }
    }
}
