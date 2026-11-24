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

    /** The ts the fake's snapshot row carries: what the engine stamped, as stored. */
    private val storedTs = "2026-11-04T11:59:58.250Z"

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val sql = json.readTree(ex.requestBody.readBytes())["sql"]?.asText().orEmpty()
                sqls += sql
                val rows =
                    when {
                        sql.startsWith("SELECT ts FROM snapshots") -> listOf(listOf(storedTs))
                        sql.startsWith("SELECT name, data_path,") ->
                            listOf(listOf("wh", "s3://b/wh", 9, 1, 2, null, null, 0))
                        else -> listOfNotNull(answer?.let { listOf(it) })
                    }
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

    /**
     * The resolution reports the ts the engine stamped on the row, read back
     * from the tenant — never a time this JVM computed. With the row stamped
     * inside the transaction (#672), the stored value is the only one that
     * says when the snapshot became visible.
     */
    @Test
    fun `snapshot resolution answers with the stamped time read from the tenant`() {
        val resolution = store.resolveSnapshot("wh", Instant.parse("2026-11-04T12:00:00Z"))

        assertThat(resolution.snapshotId).isEqualTo(4)
        assertThat(resolution.snapshotTime).isEqualTo(Instant.parse(storedTs))
        assertThat(sqls).containsExactly(
            store.metaSql,
            "SELECT id FROM snapshots WHERE ts <= ? ORDER BY id DESC LIMIT 1",
            "SELECT ts FROM snapshots WHERE id = ?",
        )
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
