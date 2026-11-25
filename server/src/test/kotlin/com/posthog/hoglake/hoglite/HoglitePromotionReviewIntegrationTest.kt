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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.atomic.AtomicInteger

/** Regression cases for review 5802232251, against real guarded transactions. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HoglitePromotionReviewIntegrationTest {
    private var started = false
    private val engine by lazy { HogliteConformanceEngine().also { started = true } }
    private val store get() = engine.store

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    private fun catalog(): String =
        HogliteConformanceEngine.uniqueName("review").also {
            store.createCatalog(it, "s3://hoglake-it/$it")
            store.createNamespace(it, "ns")
        }

    private fun throughProxy(
        before: (String, String) -> Unit,
        after: (String, String) -> Unit = { _, _ -> },
        run: (HogliteCatalogStore) -> Unit,
    ) {
        val http = HttpClient.newHttpClient()
        val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        proxy.createContext("/") { ex ->
            try {
                val body = ex.requestBody.readBytes()
                before(ex.requestURI.path, body.decodeToString())
                val response =
                    http.send(
                        HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                            .header("Content-Type", "application/json")
                            .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                after(ex.requestURI.path, body.decodeToString())
                ex.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                ex.responseBody.use { it.write(response.body()) }
            } finally {
                ex.close()
            }
        }
        proxy.start()
        try {
            HogliteClient("http://127.0.0.1:${proxy.address.port}", sleeper = {}).use {
                run(HogliteCatalogStore(it, HogliteConformanceEngine.TENANTS))
            }
        } finally {
            proxy.stop(0)
        }
    }

    private fun stats(id: Long) =
        ColumnStats(
            id,
            1,
            0,
            null,
            null,
            IcebergSingleValue.encode(ColType.INT, 3),
            IcebergSingleValue.encode(ColType.INT, 9),
        )

    private fun append(
        cat: String,
        name: String,
        fields: List<Long>,
        table: String = "t",
    ) {
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            table,
                            listOf(
                                FileRegistration("s3://hoglake-it/$cat/$name", 1, 100, 20, fields.map(::stats)),
                            ),
                        ),
                    ),
            ),
        )
    }

    @Test
    fun `promoting two columns composes one stats rewrite per file`() {
        val cat = catalog()
        store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.INT), ColumnDef("b", ColType.INT)))
        append(cat, "a", listOf(1, 2))
        store.alters.alterTable(
            cat,
            "ns",
            "t",
            listOf(
                AlterOp.PromoteColumn("a", ColType.LONG),
                AlterOp.PromoteColumn("b", ColType.LONG),
            ),
        )
        val id = store.listFiles(cat, "ns", "t").single().dataFileId
        val cols = store.fileStats(cat, "ns", "t", id).columns
        assertThat(cols).hasSize(2)
        for (col in cols) {
            assertThat(col.stats.lowerBound).hasSize(8)
            assertThat(IcebergSingleValue.decode(ColType.LONG, col.stats.lowerBound!!)).isEqualTo(3L)
            assertThat(IcebergSingleValue.decode(ColType.LONG, col.stats.upperBound!!)).isEqualTo(9L)
        }
    }

    @Test
    fun `promotion replans a racing append for guarded and blind alters`() {
        for ((guarded, seeded, atReply) in listOf(
            Triple(false, false, true),
            Triple(false, true, true),
            Triple(true, true, false),
        )) {
            val cat = catalog()
            val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.INT)))
            if (seeded) append(cat, "before", listOf(1))
            val read = store.getCatalog(cat).headSnapshotId
            val publications = AtomicInteger()
            val statsReplies = AtomicInteger()
            throughProxy({ path, body ->
                if (path.endsWith("/tx") && "table_altered" in body &&
                    publications.incrementAndGet() == 1 && !atReply
                ) {
                    append(cat, "racing", listOf(1))
                }
            }, { path, body ->
                if (atReply && path.endsWith("/query") && "SELECT id, stats_json" in body &&
                    statsReplies.incrementAndGet() == 1
                ) {
                    // The rows are already read. A fence captured after this response is too late.
                    append(cat, "racing", listOf(1))
                }
            }) { racing ->
                racing.alters.alterTable(
                    cat,
                    "ns",
                    "t",
                    listOf(AlterOp.PromoteColumn("a", ColType.LONG)),
                    if (guarded) table.tableUuid else null,
                    if (guarded) read else null,
                )
            }
            assertThat(publications.get()).isEqualTo(2)
            val files = store.listFiles(cat, "ns", "t")
            assertThat(files).hasSize(if (seeded) 2 else 1)
            for (file in files) {
                val bound = store.fileStats(cat, "ns", "t", file.dataFileId).columns.single().stats.lowerBound!!
                assertThat(bound).hasSize(8)
                assertThat(IcebergSingleValue.decode(ColType.LONG, bound)).isEqualTo(3L)
            }
        }
    }

    @Test
    fun `appends to another table do not refuse or replan a promotion`() {
        // #687: the fence was the catalog-wide file allocator, so every flush to
        // any table tripped it, and two in a row answered 409.
        val cat = catalog()
        store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.INT)))
        store.createTable(cat, "ns", "u", listOf(ColumnDef("a", ColType.INT)))
        append(cat, "before", listOf(1))
        val publications = AtomicInteger()
        val busy = AtomicInteger()
        throughProxy({ path, body ->
            if (path.endsWith("/tx") && "table_altered" in body) {
                publications.incrementAndGet()
                append(cat, "busy-${busy.incrementAndGet()}", listOf(1), table = "u")
            }
        }, { path, body ->
            if (path.endsWith("/query") && "SELECT id, stats_json" in body) {
                append(cat, "busy-${busy.incrementAndGet()}", listOf(1), table = "u")
            }
        }) { racing ->
            racing.alters.alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("a", ColType.LONG)))
        }
        assertThat(publications.get()).isEqualTo(1)
        assertThat(store.listFiles(cat, "ns", "u")).hasSize(busy.get())
        val id = store.listFiles(cat, "ns", "t").single().dataFileId
        val bound = store.fileStats(cat, "ns", "t", id).columns.single().stats.lowerBound!!
        assertThat(IcebergSingleValue.decode(ColType.LONG, bound)).isEqualTo(3L)
    }

    @Test
    fun `appends to the promoted table replan up to the file budget, then a typed 409`() {
        // #687: the file fence shared the schema fence's single replan, so two
        // appends to the promoted table during one promote answered 409.
        val budget = MAX_FILE_SET_REPLANS
        for (trips in listOf(budget, budget + 1)) {
            val cat = catalog()
            store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.INT)))
            append(cat, "before", listOf(1))
            val publications = AtomicInteger()
            val statsReplies = AtomicInteger()
            val busy = AtomicInteger()
            val result =
                runCatching {
                    throughProxy({ path, body ->
                        if (path.endsWith("/tx") && "table_altered" in body &&
                            publications.incrementAndGet() <= trips
                        ) {
                            append(cat, "busy-${busy.incrementAndGet()}", listOf(1))
                        }
                    }, { path, body ->
                        if (path.endsWith("/query") && "SELECT id, stats_json" in body &&
                            statsReplies.incrementAndGet() <= trips
                        ) {
                            append(cat, "busy-${busy.incrementAndGet()}", listOf(1))
                        }
                    }) { racing ->
                        racing.alters.alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("a", ColType.LONG)))
                    }
                }
            assertThat(publications.get()).isEqualTo(budget + 1)
            val files = store.listFiles(cat, "ns", "t")
            assertThat(files).hasSize(1 + 2 * trips)
            val widths =
                files.map { store.fileStats(cat, "ns", "t", it.dataFileId).columns.single().stats.lowerBound!!.size }
            if (trips == budget) {
                assertThat(result.exceptionOrNull()).isNull()
                assertThat(widths).containsOnly(8)
            } else {
                assertThat(result.exceptionOrNull())
                    .isInstanceOf(HoglakeException.CommitConflict::class.java)
                    .hasMessage("table ns.t changed under the alter; re-read and retry")
                assertThat(widths).containsOnly(4)
            }
        }
    }
}
