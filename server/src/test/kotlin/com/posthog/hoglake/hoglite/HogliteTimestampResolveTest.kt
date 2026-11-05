package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * AS OF timestamp resolution. Snapshot ts never decreases as the id grows
 * (insertSnapshotRow), so the newest id with ts <= T is the answer. The
 * read walks the primary key backwards, as PostgreSQL's RESOLVE_SNAPSHOT_SQL
 * does. ts has no index, so an ORDER BY ts sorts every retained snapshot.
 */
class HogliteTimestampResolveTest {
    private val json = jacksonObjectMapper()
    private val sqls = CopyOnWriteArrayList<String>()

    /** The snapshot id the fake answers, or null for no row. */
    @Volatile private var answer: Long? = 4

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                sqls += json.readTree(ex.requestBody.readBytes())["sql"]?.asText().orEmpty()
                val rows = listOfNotNull(answer?.let { listOf(it) })
                val bytes = json.writeValueAsBytes(mapOf("cols" to listOf("id"), "rows" to rows, "affected" to 0))
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            start()
        }

    private val client = HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {})
    private val store = HogliteCatalogStore(client, { "t-$it" })
    private val meta = HogliteCatalogStore.MetaRow("wh", "s3://b/wh", 9, 1, 2, null, null, false)

    @AfterEach
    fun stop() {
        client.close()
        server.stop(0)
    }

    @Test
    fun `timestamp resolution reads the newest snapshot id with ts at or before the time`() {
        val at =
            store.catalogOperation(
                "wh",
            ) { store.resolveTimestamp("wh", meta, Instant.parse("2026-11-04T12:00:00Z")) }

        assertThat(at).isEqualTo(4)
        assertThat(sqls.single()).isEqualTo("SELECT id FROM snapshots WHERE ts <= ? ORDER BY id DESC LIMIT 1")
    }

    @Test
    fun `a time before every retained snapshot is Expired`() {
        answer = null

        assertThat(
            catchThrowable { store.catalogOperation("wh") { store.resolveTimestamp("wh", meta, Instant.EPOCH) } },
        )
            .isInstanceOf(HoglakeException.Expired::class.java)
            .hasMessageContaining("earliest retained snapshot is 2")
    }
}
