package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/** Model the engine's earlier snapshot pin and later read-worker execution. */
class HogliteCreationReadCutTest {
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `an old prepared snapshot cannot become expired after an on-time publisher commits`(persistExpiry: Boolean) {
        val json = jacksonObjectMapper()
        val operation = UUID.randomUUID()
        val table = UUID.randomUUID()
        val beforeExpiry = Instant.parse("2000-01-01T00:00:00Z")
        val deadline = beforeExpiry.plusSeconds(60)

        fun julian(time: Instant): Double = time.toEpochMilli() / 86_400_000.0 + 2_440_587.5
        val clock = AtomicReference(beforeExpiry)
        val state = AtomicReference("prepared")
        val pinned = CountDownLatch(1)
        val resume = CountDownLatch(1)
        val first = AtomicBoolean(true)
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = executor
        server.createContext("/") { exchange ->
            try {
                val request = json.readTree(exchange.requestBody.readBytes())
                val body =
                    if (exchange.requestURI.path.endsWith("/tx")) {
                        mapOf(
                            "results" to
                                request["statements"].map {
                                    mapOf("cols" to listOf("guard"), "rows" to listOf(listOf(1)), "affected" to 0)
                                },
                        )
                    } else {
                        val sql = request["sql"].asText()
                        val row =
                            if ("FROM table_creations" !in sql) {
                                listOf(julian(clock.get()))
                            } else {
                                val snapshotState = state.get()
                                if (first.compareAndSet(true, false)) {
                                    pinned.countDown()
                                    check(resume.await(10, TimeUnit.SECONDS))
                                }
                                val values =
                                    mutableListOf<Any?>(
                                        table.toString(),
                                        TableCreationDefinitionCodec.encode(
                                            TableCreationDefinition(
                                                "ns",
                                                "pending",
                                                listOf(ColumnDef("id", ColType.LONG)),
                                            ),
                                        ),
                                        "s3://bucket/data/$table/",
                                        snapshotState,
                                        HogliteSchema.fmt(deadline),
                                        null,
                                        null,
                                        null,
                                    )
                                // A SQL-now overlay runs after its old snapshot was pinned.
                                val expiryCut =
                                    if (HogliteSchema.CREATION_NOW_SQL in sql) {
                                        julian(clock.get())
                                    } else {
                                        request["args"][0].asDouble()
                                    }
                                val expired =
                                    "CASE WHEN" in sql && snapshotState == "prepared" && julian(deadline) <= expiryCut
                                values.add(if (expired) 1 else 0)
                                values
                            }
                        mapOf("cols" to row.indices.map { "c$it" }, "rows" to listOf(row), "affected" to 0)
                    }
                val bytes = json.writeValueAsBytes(body)
                exchange.sendResponseHeaders(200, bytes.size.toLong())
                exchange.responseBody.use { it.write(bytes) }
            } finally {
                exchange.close()
            }
        }
        server.start()
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}").use { client ->
                val store = HogliteCatalogStore(client, { "opaque" })
                val pending =
                    executor.submit<com.posthog.hoglake.service.TableCreation> {
                        store.catalogOperation("cat") { store.loadCreationOrThrow("cat", operation, persistExpiry) }
                    }
                assertThat(pinned.await(10, TimeUnit.SECONDS)).isTrue()
                // The publisher commits before the deadline.
                // This reader retains its prepared snapshot.
                state.set("committed")
                clock.set(deadline.plusSeconds(60))
                resume.countDown()
                assertThat(pending.get(10, TimeUnit.SECONDS).state).isEqualTo("prepared")
                assertThat(store.tableCreations.status("cat", operation).state).isEqualTo("committed")
            }
        } finally {
            resume.countDown()
            server.stop(0)
            executor.close()
        }
    }
}
