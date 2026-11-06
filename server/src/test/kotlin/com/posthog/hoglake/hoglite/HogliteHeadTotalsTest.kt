package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.UUID

/**
 * Head totals must describe readSnapshotId. The fake shows a truncate that
 * lands after the meta read: meta says head 5, but table_totals is already
 * at snapshot 6 (zero files). The files visible at 5 hold 10 rows.
 */
class HogliteHeadTotalsTest {
    private val json = jacksonObjectMapper()
    private val uuid = UUID.randomUUID().toString()

    /** The head meta() reports. 6 means no write landed after the meta read. */
    @Volatile private var metaHead = 5

    private val sqls = java.util.concurrent.CopyOnWriteArrayList<String>()

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val sql = json.readTree(ex.requestBody.readBytes())["sql"]?.asText().orEmpty()
                sqls += sql
                val row: List<Any?>? =
                    when {
                        sql.startsWith(
                            "SELECT name, data_path,",
                        ) -> listOf("wh", "s3://b/wh", metaHead, 0, 0, null, null, 0)
                        sql.startsWith("SELECT id, name, contained_field, contained_value FROM namespaces") ->
                            listOf(1, "ns", null, null)
                        sql.startsWith("SELECT t.id, t.table_uuid, v.namespace_id") ->
                            listOf(7, uuid, 1, "ns", "t", null, "{}", 10, 0)
                        // The pinned listing: totals aggregated at the snapshot.
                        sql.startsWith("SELECT t.id, t.table_uuid, v.name") && sql.contains("SUM(f.record_count)") ->
                            listOf(7, uuid, "t", null, 10, 1, 100, 1, 1, 6)
                        // The head listing: current table_totals, after the truncate.
                        sql.startsWith(
                            "SELECT t.id, t.table_uuid, v.name",
                        ) -> listOf(7, uuid, "t", null, 0, 0, 0, 1, 1, 6)
                        sql.contains("table_totals") -> listOf(0, 0, 0, 6)
                        sql.contains("FROM files WHERE table_id = ?") -> listOf(10, 1, 100)
                        else -> null
                    }
                val answer =
                    mapOf("cols" to List(row?.size ?: 0) { "c$it" }, "rows" to listOfNotNull(row), "affected" to 0)
                val bytes = json.writeValueAsBytes(answer)
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            start()
        }

    private val client = HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {})
    private val store = HogliteCatalogStore(client, { "t-$it" })

    @AfterEach
    fun stop() {
        client.close()
        server.stop(0)
    }

    @Test
    fun `head getTable totals describe readSnapshotId when a truncate lands after the meta read`() {
        val info = store.getTable("wh", "ns", "t", null, null, totals = true)

        assertThat(info.readSnapshotId).isEqualTo(5)
        assertThat(listOf(info.recordCount, info.fileCount, info.fileSizeBytes)).containsExactly(10L, 1L, 100L)
    }

    @Test
    fun `head listTables totals describe the listing snapshot when a truncate lands after the meta read`() {
        val summary = store.listTables("wh", "ns", null, null).single()

        assertThat(listOf(summary.recordCount, summary.fileCount, summary.fileSizeBytes)).containsExactly(10L, 1L, 100L)
    }

    @Test
    fun `head totals come from table_totals in one read when the head has not moved`() {
        metaHead = 6

        val info = store.getTable("wh", "ns", "t", null, null, totals = true)
        val summary = store.listTables("wh", "ns", null, null).single()

        assertThat(listOf(info.recordCount, info.fileCount, info.fileSizeBytes)).containsExactly(0L, 0L, 0L)
        assertThat(listOf(summary.recordCount, summary.fileCount, summary.fileSizeBytes)).containsExactly(0L, 0L, 0L)
        assertThat(
            sqls,
        ).noneMatch { it.contains("FROM files WHERE table_id = ?") || it.contains("SUM(f.record_count)") }
    }
}
