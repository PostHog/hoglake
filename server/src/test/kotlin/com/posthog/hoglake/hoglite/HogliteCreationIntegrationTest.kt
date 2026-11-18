package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** Prepared creation refusals and abort races use the live engine and MinIO. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCreationIntegrationTest {
    private var started = false
    private val engine by lazy { HogliteConformanceEngine().also { started = true } }
    private val store get() = engine.store

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    private fun catalog(): String {
        val cat = "retire" + UUID.randomUUID().toString().replace("-", "")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        store.createNamespace(cat, "ns")
        return cat
    }

    private fun write(
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(cat) {
        store.runTx(cat, HogliteCatalogStore.freshTxid("fixture"), null, HogliteTx().apply { add(sql, *args) })
    }

    private fun query(
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(cat) {
        store.queryCatalog(cat, sql, *args)
    }

    private fun invalidPrepared(cat: String): UUID {
        val op = UUID.randomUUID()
        val definition = TableCreationDefinition("ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.tableCreations.prepare(cat, op, definition)
        // An older server accepted a now-invalid definition; publication must reject it durably.
        write(
            cat,
            "UPDATE table_creations SET definition_json = ? WHERE operation = ?",
            TableCreationDefinitionCodec.encode(definition.copy(properties = mapOf("hoglake.reserved" to "x"))),
            op.toString(),
        )
        return op
    }

    @Test
    fun `invalid prepared definition rejects durably without racing abort`() {
        val cat = catalog()
        val op = invalidPrepared(cat)
        val rejected = store.tableCreations.publish(cat, op, emptyList())
        assertThat(rejected.state).isEqualTo("rejected")
        assertThat(rejected.reason).isEqualTo("definition_invalid")
        assertThat(store.tableCreations.publish(cat, op, emptyList()).state).isEqualTo("rejected")
        assertThat(store.listTables(cat, "ns")).isEmpty()
    }

    @Test
    fun `abort wins the invalid definition publication transition`() {
        val cat = catalog()
        val op = invalidPrepared(cat)
        val intercepted = AtomicInteger()
        val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val http = HttpClient.newHttpClient()
        proxy.createContext("/") { exchange ->
            try {
                val body = exchange.requestBody.readBytes()
                if (exchange.requestURI.path.endsWith("/tx") &&
                    body.decodeToString().contains("UPDATE table_creations SET files_json")
                ) {
                    intercepted.incrementAndGet()
                    assertThat(store.tableCreations.abort(cat, op).state).isEqualTo("aborted")
                }
                val response =
                    http.send(
                        HttpRequest.newBuilder(URI.create(engine.url() + exchange.requestURI))
                            .header("Content-Type", "application/json")
                            .method(exchange.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                exchange.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                exchange.responseBody.use { it.write(response.body()) }
            } finally {
                exchange.close()
            }
        }
        proxy.start()
        try {
            HogliteClient("http://127.0.0.1:${proxy.address.port}", sleeper = {}).use { client ->
                val racing = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
                val result = racing.tableCreations.publish(cat, op, emptyList())
                assertThat(intercepted.get()).isEqualTo(1)
                assertThat(result.state).isEqualTo("aborted")
                assertThat(result.reason).isEqualTo("client_abort")
            }
        } finally {
            proxy.stop(0)
        }
        val durable =
            query(
                cat,
                "SELECT state, reason, files_json FROM table_creations WHERE operation = ?",
                op.toString(),
            ).rows.single()
        assertThat(durable).containsExactly("aborted", "client_abort", null)
        assertThat(store.listTables(cat, "ns")).isEmpty()
    }

    @Test
    fun `ordinary commit validates footer and delete count boundaries before publication`() {
        val cat = catalog()
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val before = store.getCatalog(cat).headSnapshotId
        for ((i, footer) in listOf(-1L, 93L, 92L, 0L).withIndex()) {
            val request =
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(
                                    FileRegistration("s3://hoglake-it/$cat/footer$i", 1, 100, footer),
                                ),
                            ),
                        ),
                )
            if (footer < 0 || footer > 92) {
                assertThatThrownBy { store.commit(cat, request) }
                    .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                    .hasMessage("invalid footer_size")
                assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
            } else {
                store.commit(cat, request)
            }
        }
        val files = store.listFiles(cat, "ns", "t")
        assertThat(files).hasSize(2)
        val dataId = files.first().dataFileId
        for (count in listOf(0L, -1L, 1L)) {
            val at = store.getCatalog(cat).headSnapshotId
            val request =
                CommitRequest(
                    readSnapshot = at,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(dataId, "s3://hoglake-it/$cat/dv$count", count, 20),
                                ),
                            ),
                        ),
                )
            if (count <= 0) {
                assertThatThrownBy { store.commit(cat, request) }
                    .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                    .hasMessage("delete_count must be > 0 for data_file_id $dataId in ns.t, got $count")
                assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(at)
            } else {
                store.commit(cat, request)
            }
        }
    }
}
