package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.stats.IcebergSingleValue
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/** Classified byte-budget faults, not raised engine limits or a fake database. */
@Tag("integration")
class HoglitePromotionByteBudgetIntegrationTest {
    @Test
    fun `stats pages narrow on byte refusals but stop if one row cannot fit`() {
        HogliteConformanceEngine().use { engine ->
            val store = engine.store
            val cat = HogliteConformanceEngine.uniqueName("stats_bytes")
            store.createCatalog(cat, "s3://hoglake-it/$cat")
            store.createNamespace(cat, "ns")
            for (table in listOf("t", "too_wide")) {
                store.createTable(cat, "ns", table, listOf(ColumnDef("a", ColType.INT)))
                val bound = IcebergSingleValue.encode(ColType.INT, 7)
                store.commit(
                    cat,
                    CommitRequest(
                        appends =
                            listOf(
                                TableAppend(
                                    "ns",
                                    table,
                                    (1..5).map { id ->
                                        FileRegistration(
                                            "s3://hoglake-it/$cat/$table/$id",
                                            1,
                                            100,
                                            20,
                                            listOf(ColumnStats(1, 1, 0, null, null, bound, bound)),
                                        )
                                    },
                                ),
                            ),
                    ),
                )
            }
            val limits = CopyOnWriteArrayList<Int>()
            val refuseOne = AtomicBoolean(false)
            val writes = AtomicInteger()
            val http = HttpClient.newHttpClient()
            val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            proxy.createContext("/") { ex ->
                try {
                    val body = ex.requestBody.readBytes()
                    if (ex.requestURI.path.endsWith("/query")) {
                        val json = store.json.readTree(body)
                        if ("stats_json IS NOT NULL" in json.path("sql").asText()) {
                            val limit = json.path("args").last().asInt()
                            limits += limit
                            if (limit > 2 || refuseOne.get()) {
                                val refusal =
                                    """{"code":"result_too_large","error":"injected byte cap"}""".toByteArray()
                                ex.sendResponseHeaders(413, refusal.size.toLong())
                                ex.responseBody.use { it.write(refusal) }
                                return@createContext
                            }
                        }
                    } else if (ex.requestURI.path.endsWith("/tx") && "table_altered" in body.decodeToString()) {
                        writes.incrementAndGet()
                    }
                    val response =
                        http.send(
                            HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                                .header("Content-Type", "application/json")
                                .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                            HttpResponse.BodyHandlers.ofByteArray(),
                        )
                    ex.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                    ex.responseBody.use { it.write(response.body()) }
                } finally {
                    ex.close()
                }
            }
            proxy.start()
            try {
                HogliteClient("http://127.0.0.1:${proxy.address.port}").use { client ->
                    val selected = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
                    selected.alters.alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("a", ColType.LONG)))
                    assertThat(limits.take(8)).containsExactly(256, 128, 64, 32, 16, 8, 4, 2)
                    assertThat(limits.drop(8)).allMatch { it == 2 }
                    val files = store.listFiles(cat, "ns", "t")
                    assertThat(files).hasSize(5)
                    files.forEach { file ->
                        assertThat(store.fileStats(cat, "ns", "t", file.dataFileId).columns.single().stats.lowerBound)
                            .hasSize(8)
                    }
                    limits.clear()
                    refuseOne.set(true)
                    val before = store.getCatalog(cat).headSnapshotId
                    assertThatThrownBy {
                        selected.alters.alterTable(
                            cat,
                            "ns",
                            "too_wide",
                            listOf(AlterOp.PromoteColumn("a", ColType.LONG)),
                        )
                    }.isExactlyInstanceOf(HoglakeException.BackendResultTooLarge::class.java)
                    assertThat(limits).containsExactly(256, 128, 64, 32, 16, 8, 4, 2, 1)
                    assertThat(writes.get()).isEqualTo(1)
                    assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
                    assertThat(store.getTable(cat, "ns", "too_wide").columns.single().def.type).isEqualTo(ColType.INT)
                }
            } finally {
                proxy.stop(0)
            }
        }
    }
}
