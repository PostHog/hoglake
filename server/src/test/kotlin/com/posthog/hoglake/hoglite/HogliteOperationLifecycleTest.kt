package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicInteger

/** Transport-boundary schedules: no opt-in binding switch, no tenant state fabricated in a store. */
class HogliteOperationLifecycleTest {
    private fun fixture(body: (HogliteCatalogStore, AtomicInteger) -> Unit) {
        val json = jacksonObjectMapper()
        val calls = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { ex ->
            calls.incrementAndGet()
            val request = json.readTree(ex.requestBody.readBytes())
            val sql = request["sql"]?.asText().orEmpty()
            val missing = sql == "SELECT bound_absence"
            val result = mapOf("cols" to listOf("c"), "rows" to listOf(listOf(1)), "affected" to 0)
            val answer: Any =
                when {
                    missing -> mapOf("error" to "tenant does not exist")
                    ex.requestURI.path.contains(
                        "__hoglake_registry",
                    ) -> mapOf("cols" to listOf("tenant"), "rows" to listOf(listOf("captured")))
                    sql.startsWith(
                        "SELECT (SELECT COUNT(*) FROM sqlite_master",
                    ) ->
                        mapOf(
                            "cols" to
                                List(HogliteSchema.TENANT_MIGRATIONS.size) {
                                    "c"
                                },
                            "rows" to listOf(List(HogliteSchema.TENANT_MIGRATIONS.size) { 1 }),
                        )
                    request.has("statements") -> mapOf("results" to request["statements"].map { result })
                    else -> result
                }
            val bytes = json.writeValueAsBytes(answer)
            ex.sendResponseHeaders(if (missing) 404 else 200, bytes.size.toLong())
            ex.responseBody.use { it.write(bytes) }
        }
        server.start()
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { client ->
                body(HogliteCatalogStore(client), calls)
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `unbound tenant query read and transaction fail without any transport or flag`() =
        fixture { store, calls ->
            val actions =
                listOf<() -> Unit>(
                    { store.queryCatalog("cat", "SELECT 1") },
                    { store.readCatalog("cat", listOf(HogliteStatement("SELECT 1"))) },
                    { store.runTx("cat", "unbound", null, HogliteTx().apply { add("SELECT 1") }) },
                )
            for (action in actions) {
                assertThat(
                    catchThrowable {
                        action()
                    },
                ).isInstanceOf(IllegalStateException::class.java).hasMessageContaining("operation binding")
                assertThat(calls.get()).isZero()
            }
        }

    @Test
    fun `unknown outcome retains original txid when finalizer has binding poison`() =
        fixture { store, _ ->
            val original = HoglakeException.CommitOutcomeUnknown("original-txid", "ambiguous write")
            var poison: Throwable? = null
            val failure =
                catchThrowable {
                    store.catalogOperation("cat") {
                        poison = catchThrowable { store.queryCatalog("cat", "SELECT bound_absence") }
                        throw original
                    }
                }
            assertThat(failure).isSameAs(original)
            assertThat((failure as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo("original-txid")
            assertThat(failure.suppressed).containsExactly(poison)
            assertUnboundAndFresh(store)
        }

    @Test
    fun `swallowed bound absence still refuses normal success`() =
        fixture { store, calls ->
            var poison: Throwable? = null
            val failure =
                catchThrowable {
                    store.catalogOperation("cat") {
                        poison = catchThrowable { store.queryCatalog("cat", "SELECT bound_absence") }
                        val count = calls.get()
                        assertThat(catchThrowable { store.queryCatalog("cat", "SELECT 1") }).isSameAs(poison)
                        assertThat(calls.get()).isEqualTo(count)
                        "not success"
                    }
                }
            assertThat(failure).isSameAs(poison).isInstanceOf(HoglakeException.NotFound::class.java)
            assertUnboundAndFresh(store)
        }

    @Test
    fun `nested poison preserves body error once without self suppression`() =
        fixture { store, _ ->
            val original = IllegalArgumentException("body")
            var poison: Throwable? = null
            val failure =
                catchThrowable {
                    store.catalogOperation("cat") {
                        store.catalogOperation("cat") {
                            poison = catchThrowable { store.queryCatalog("cat", "SELECT bound_absence") }
                            throw original
                        }
                    }
                }
            assertThat(failure).isSameAs(original)
            assertThat(failure.suppressed).containsExactly(poison)
            assertUnboundAndFresh(store)
            val direct =
                catchThrowable { store.catalogOperation("cat") { store.queryCatalog("cat", "SELECT bound_absence") } }
            assertThat(direct).isInstanceOf(HoglakeException.NotFound::class.java)
            assertThat(direct.suppressed).isEmpty()
            assertUnboundAndFresh(store)
        }

    @ParameterizedTest
    @ValueSource(strings = ["interrupt", "cancel", "fatal"])
    fun `interruption cancellation and fatal Error survive finalizer poison`(kind: String) =
        fixture { store, _ ->
            val original =
                when (kind) {
                    "interrupt" -> InterruptedException("interrupt")
                    "cancel" -> CancellationException("cancel")
                    else -> AssertionError("fatal")
                }
            run {
                var poison: Throwable? = null
                try {
                    val failure =
                        catchThrowable {
                            store.catalogOperation("cat") {
                                poison = catchThrowable { store.queryCatalog("cat", "SELECT bound_absence") }
                                if (original is InterruptedException) Thread.currentThread().interrupt()
                                throw original
                            }
                        }
                    assertThat(failure).isSameAs(original)
                    assertThat(failure.suppressed).containsExactly(poison)
                    assertThat(Thread.currentThread().isInterrupted).isEqualTo(original is InterruptedException)
                } finally {
                    Thread.interrupted()
                }
                assertUnboundAndFresh(store)
            }
        }

    private fun early(store: HogliteCatalogStore): String = store.catalogOperation("cat") { return "early" }

    @Test
    fun `normal nested early return and body throw always clear operation scope`() =
        fixture { store, _ ->
            assertThat(early(store)).isEqualTo("early")
            assertUnboundAndFresh(store)
            store.catalogOperation("cat") {
                val tenant = store.boundTenant("cat")
                store.catalogOperation("cat") { assertThat(store.boundTenant("cat")).isEqualTo(tenant) }
                assertThat(store.boundTenant("cat")).isEqualTo(tenant)
                store.catalogOperation("other", tenant = "other-captured") {
                    assertThat(store.boundTenant("other")).isEqualTo("other-captured")
                }
                assertThat(
                    catchThrowable { store.boundTenant("other") },
                ).isInstanceOf(IllegalStateException::class.java)
            }
            assertUnboundAndFresh(store)
            val original = IllegalStateException("body")
            assertThat(catchThrowable { store.catalogOperation("cat") { throw original } }).isSameAs(original)
            assertThat(original.suppressed).isEmpty()
            assertUnboundAndFresh(store)
        }

    private fun assertUnboundAndFresh(store: HogliteCatalogStore) {
        assertThat(catchThrowable { store.boundTenant("cat") }).isInstanceOf(IllegalStateException::class.java)
        store.catalogOperation("cat") {
            assertThat(store.queryCatalog("cat", "SELECT 1").firstRow()!![0].asLong()).isEqualTo(1)
        }
        assertThat(catchThrowable { store.boundTenant("cat") }).isInstanceOf(IllegalStateException::class.java)
    }
}
