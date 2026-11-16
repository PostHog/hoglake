package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Head totals must describe readSnapshotId (#537). `table_totals` and the
 * head are the only unversioned state, so a head read must take both from
 * ONE engine snapshot: one read-only `/tx`, never a `/query` of meta
 * followed by a later read of `table_totals`.
 *
 * The fake has two snapshots. A truncate landed at 6 (zero files); the
 * files visible at 5 hold 10 rows. A standalone `/query` of meta answers
 * 5, as a read taken before the truncate would. A `/tx` answers every
 * statement at 6, as one engine snapshot does.
 */
class HogliteHeadTotalsTest {
    private val json = jacksonObjectMapper()
    private val uuid = UUID.randomUUID().toString()

    /** One request as the fake saw it: its endpoint and its statements. */
    private class Request(
        val tx: Boolean,
        val sqls: List<String>,
    )

    private val requests = CopyOnWriteArrayList<Request>()

    /** [sql]'s one row (or none), at 6 inside a `/tx`, at 5 on a `/query`. */
    private fun answer(
        sql: String,
        tx: Boolean,
    ): List<Any?>? =
        when {
            sql.startsWith("SELECT name, data_path,") ->
                listOf("wh", "s3://b/wh", if (tx) 6 else 5, 0, 0, null, null, 0)
            sql == "SELECT earliest_snapshot_id FROM meta WHERE id = 1" -> listOf(0)
            sql.startsWith("SELECT id, name, contained_field, contained_value FROM namespaces") ->
                listOf(1, "ns", null, null)
            // The head table read: the table row, the head it saw, then its table_totals.
            sql.startsWith("SELECT t.id, t.table_uuid, v.namespace_id") && sql.contains("table_totals") ->
                listOf(7, uuid, 1, "ns", "t", null, "{}", 10, 0, 6, 0, 0, 0)
            sql.startsWith("SELECT t.id, t.table_uuid, v.namespace_id") ->
                listOf(7, uuid, 1, "ns", "t", null, "{}", 10, 0)
            // The pinned listing: totals aggregated at the snapshot.
            sql.startsWith("SELECT t.id, t.table_uuid, v.name") && sql.contains("SUM(f.record_count)") ->
                listOf(7, uuid, "t", null, 10, 1, 100, 1, 1, 6)
            // The head listing: table_totals at 6, after the truncate.
            sql.startsWith("SELECT t.id, t.table_uuid, v.name") -> listOf(7, uuid, "t", null, 0, 0, 0, 1, 1, 6)
            sql.contains("FROM files WHERE table_id = ?") -> listOf(10, 1, 100)
            else -> null
        }

    private fun result(row: List<Any?>?) =
        mapOf("cols" to List(row?.size ?: 0) { "c$it" }, "rows" to listOfNotNull(row), "affected" to 0)

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val body = json.readTree(ex.requestBody.readBytes())
                val tx = ex.requestURI.path.endsWith("/tx")
                val sqls = if (tx) body["statements"].map { it["sql"].asText() } else listOf(body["sql"].asText())
                requests += Request(tx, sqls)
                val answer =
                    if (tx) {
                        mapOf("results" to sqls.map { result(answer(it, tx = true)) })
                    } else {
                        result(answer(sqls.single(), tx = false))
                    }
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

    /**
     * The head read's shape: every statement that reads `table_totals`
     * shares one `/tx` with the meta read, no meta read is a `/query` of
     * its own, and nothing aggregates the files.
     */
    private fun assertOneSnapshotHeadRead() {
        val totalsReads = requests.filter { r -> r.sqls.any { it.contains("table_totals") } }
        assertThat(totalsReads).isNotEmpty().allSatisfy { r ->
            assertThat(r.tx).isTrue()
            assertThat(r.sqls).anyMatch { it.startsWith("SELECT name, data_path,") }
        }
        assertThat(requests).noneMatch { !it.tx && it.sqls.single().startsWith("SELECT name, data_path,") }
        assertThat(requests.flatMap { it.sqls })
            .noneMatch { it.contains("FROM files WHERE table_id = ?") || it.contains("SUM(f.record_count)") }
    }

    @Test
    fun `head getTable reads the head and table_totals in one snapshot`() {
        val info = store.getTable("wh", "ns", "t", null, null, totals = true)

        assertThat(info.readSnapshotId).isEqualTo(6)
        assertThat(listOf(info.recordCount, info.fileCount, info.fileSizeBytes)).containsExactly(0L, 0L, 0L)
        assertOneSnapshotHeadRead()
    }

    @Test
    fun `head listTables reads the head and table_totals in one snapshot`() {
        val summary = store.listTables("wh", "ns", null, null).single()

        assertThat(listOf(summary.recordCount, summary.fileCount, summary.fileSizeBytes)).containsExactly(0L, 0L, 0L)
        assertOneSnapshotHeadRead()
    }

    @Test
    fun `pinned getTable totals aggregate the files visible at the snapshot`() {
        val info = store.getTable("wh", "ns", "t", 5, null, totals = true)

        assertThat(info.readSnapshotId).isEqualTo(5)
        assertThat(listOf(info.recordCount, info.fileCount, info.fileSizeBytes)).containsExactly(10L, 1L, 100L)
        assertThat(requests).noneMatch { it.tx }
    }

    @Test
    fun `pinned listTables totals aggregate the files visible at the snapshot`() {
        val summary = store.listTables("wh", "ns", 5, null).single()

        assertThat(listOf(summary.recordCount, summary.fileCount, summary.fileSizeBytes)).containsExactly(10L, 1L, 100L)
        assertThat(requests).noneMatch { it.tx }
    }
}
