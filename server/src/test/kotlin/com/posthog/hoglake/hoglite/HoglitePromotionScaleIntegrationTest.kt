package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.HoglakeException
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

/** Default engine limits, >100k actual rewrites, retained rows, and a late atomic rollback. */
@Tag("integration")
class HoglitePromotionScaleIntegrationTest {
    @Test
    fun `promotion pages beyond the read cap and publishes every retained rewrite atomically`() {
        HogliteConformanceEngine().use { engine ->
            val store = engine.store
            val cat = HogliteConformanceEngine.uniqueName("promote_scale")
            store.createCatalog(cat, "s3://hoglake-it/$cat")
            store.createNamespace(cat, "ns")
            val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.INT)))
            store.createNamespace(cat, "later")
            val before = store.getCatalog(cat).headSnapshotId
            val count = 100_001
            val ended = 50_000
            val bound = IcebergSingleValue.encode(ColType.INT, 7)

            fun stats(nulls: Long) =
                store.json.writeValueAsString(
                    listOf(ColumnStats(1, 1, nulls, null, null, bound, bound)),
                )
            store.catalogOperation(cat) {
                for (first in 1..count step 5000) {
                    store.runTx(
                        cat,
                        HogliteCatalogStore.freshTxid("promotion-scale"),
                        null,
                        HogliteTx().apply {
                            add(
                                "WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?) " +
                                    "INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start," +
                                    "stats_state,stats_json,begin_snapshot,end_snapshot) " +
                                    "SELECT i,?,'s3://hoglake-it/$cat/'||i,1,100,i,'provided',?,?," +
                                    "CASE WHEN i <= ? THEN ? ELSE NULL END FROM n",
                                first,
                                minOf(first + 4999, count),
                                table.tableId,
                                stats(0),
                                table.readSnapshotId,
                                ended,
                                before,
                            )
                        },
                    )
                }
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("promotion-scale-meta"),
                    null,
                    HogliteTx().apply {
                        add("UPDATE meta SET next_file_id = ? WHERE id = 1", count + 1)
                        add(
                            "UPDATE table_totals SET record_count = ?,file_count = ?,file_size_bytes = ? " +
                                "WHERE table_id = ?",
                            count - ended,
                            count - ended,
                            (count - ended) * 100L,
                            table.tableId,
                        )
                    },
                )
            }
            val pages = CopyOnWriteArrayList<String>()
            val limits = CopyOnWriteArrayList<Int>()
            val publish = AtomicInteger()
            val inject = AtomicBoolean(true)
            val http = HttpClient.newHttpClient()
            val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            proxy.createContext("/") { ex ->
                try {
                    val body = ex.requestBody.readBytes()
                    if (ex.requestURI.path.endsWith("/query")) {
                        val json = store.json.readTree(body)
                        val sql = json.path("sql").asText()
                        if ("stats_json IS NOT NULL" in sql) {
                            pages += sql
                            limits += json.path("args").last().asInt()
                        }
                    } else if (ex.requestURI.path.endsWith("/tx") && "table_altered" in body.decodeToString()) {
                        publish.incrementAndGet()
                        if (inject.compareAndSet(true, false)) {
                            store.catalogOperation(cat) {
                                store.runTx(
                                    cat,
                                    HogliteCatalogStore.freshTxid("late-stats-change"),
                                    null,
                                    HogliteTx().apply {
                                        add("UPDATE files SET stats_json = ? WHERE id = ?", stats(1), count)
                                    },
                                )
                            }
                        }
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
                // Only the transport deadline is extended for this expensive correctness fixture.
                // The engine's row, byte, and execution limits are unchanged.
                HogliteClient(
                    "http://127.0.0.1:${proxy.address.port}",
                    commitTimeoutMs = 120_000,
                    readTimeoutMs = 30_000,
                    retryBudgetMs = 120_000,
                ).use { client ->
                    val selected = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
                    assertThatThrownBy {
                        selected.alters.alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("a", ColType.LONG)))
                    }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
                        .hasMessage("file stats changed under the alter")
                    assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
                    assertThat(store.getTable(cat, "ns", "t").columns.single().def.type).isEqualTo(ColType.INT)
                    assertBounds(store, cat, count.toLong(), 8)
                    val result =
                        selected.alters.alterTable(
                            cat,
                            "ns",
                            "t",
                            listOf(AlterOp.PromoteColumn("a", ColType.LONG)),
                        )
                    assertThat(result.columns.single().def.type).isEqualTo(ColType.LONG)
                    assertThat(result.readSnapshotId).isEqualTo(before + 1)
                    assertBounds(store, cat, count.toLong(), 12)
                    assertThat(publish.get()).isEqualTo(2)
                    assertThat(pages.size).isGreaterThan(780)
                    assertThat(pages).allMatch { "LIMIT ?" in it && "id < ?" in it }
                    assertThat(limits).allMatch { it in 1..256 }
                }
            } finally {
                proxy.stop(0)
            }
        }
    }

    private fun assertBounds(
        store: HogliteCatalogStore,
        cat: String,
        expected: Long,
        encodedWidth: Int,
    ) {
        store.catalogOperation(cat) {
            val row =
                store.queryCatalog(
                    cat,
                    "SELECT COUNT(*), SUM(CASE WHEN length(json_extract(stats_json,'$[0].lowerBound')) = ? " +
                        "AND length(json_extract(stats_json,'$[0].upperBound')) = ? THEN 1 ELSE 0 END), " +
                        "SUM(CASE WHEN end_snapshot IS NOT NULL THEN 1 ELSE 0 END) FROM files",
                    encodedWidth,
                    encodedWidth,
                ).firstRow()!!
            assertThat(row[0].asLong()).isEqualTo(expected)
            assertThat(row[1].asLong()).isEqualTo(expected)
            assertThat(row[2].asLong()).isEqualTo(50_000)
        }
    }
}
