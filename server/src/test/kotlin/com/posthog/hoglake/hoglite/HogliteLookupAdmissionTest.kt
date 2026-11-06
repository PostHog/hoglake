package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class HogliteLookupAdmissionTest {
    private fun exercise(
        activeTenants: List<String>,
        refusedTenant: String,
        refusedName: String,
    ) {
        val entered = CountDownLatch(activeTenants.size)
        val release = CountDownLatch(1)
        val reads = AtomicInteger()
        val handlers = Executors.newCachedThreadPool()
        val calls = Executors.newFixedThreadPool(activeTenants.size)
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = handlers
        server.createContext("/") { ex ->
            try {
                ex.requestBody.readBytes()
                if (reads.incrementAndGet() <= activeTenants.size) {
                    entered.countDown()
                    check(release.await(10, TimeUnit.SECONDS))
                }
                val response = "{\"error\":\"missing tenant\"}".toByteArray()
                ex.sendResponseHeaders(404, response.size.toLong())
                ex.responseBody.use { it.write(response) }
            } finally {
                ex.close()
            }
        }
        server.start()
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {}).use { client ->
                val admission = HoglitePartitionLookupAdmission()

                fun store(tenant: String) =
                    HogliteCatalogStore(
                        client,
                        { tenant },
                        partitionLookupAdmission = admission,
                    )
                val futures =
                    activeTenants.map { tenant ->
                        calls.submit<Any> { store(tenant).partitionLookup.lookup("cat", "ns", "t", 1, listOf("x")) }
                    }
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
                assertThatThrownBy {
                    store(refusedTenant).partitionLookup.lookup(refusedName, "ns", "t", 1, listOf("x"))
                }.isExactlyInstanceOf(HoglakeException.PartitionLookupUnavailable::class.java)
                    .hasFieldOrPropertyWithValue("reason", "busy")
                assertThat(reads.get()).isEqualTo(activeTenants.size)
                release.countDown()
                futures.forEach { future ->
                    assertThatThrownBy { future.get(5, TimeUnit.SECONDS) }
                        .isExactlyInstanceOf(ExecutionException::class.java)
                        .hasCauseInstanceOf(HoglakeException.NotFound::class.java)
                }
                // A failed admitted query must release both its tenant and backend capacity.
                assertThatThrownBy {
                    store(refusedTenant).partitionLookup.lookup(refusedName, "ns", "t", 1, listOf("x"))
                }.isExactlyInstanceOf(HoglakeException.NotFound::class.java)
            }
        } finally {
            release.countDown()
            server.stop(0)
            calls.shutdownNow()
            handlers.shutdownNow()
        }
    }

    @Test
    fun `same tenant aliases share admission across request stores`() =
        exercise(
            listOf("opaque_a"),
            "opaque_a",
            "alias",
        )

    @Test
    fun `backend capacity spans distinct tenants and request stores`() =
        exercise(listOf("opaque_a", "opaque_b"), "opaque_c", "cat")
}
