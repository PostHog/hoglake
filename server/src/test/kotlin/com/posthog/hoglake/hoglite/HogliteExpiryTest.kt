package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.PartialResult
import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.time.DateTimeException
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The retention cutoff of [HogliteExpiry]'s floor advance, against an
 * in-process fake of hoglite's HTTP API. The sweep compares snapshot ts
 * with a cutoff on the ENGINE's clock, read in the same query as the
 * floor candidate, so the pod's clock never reaches the predicate (#672)
 * and a cutoff SQLite cannot represent (NULL, where PostgreSQL's
 * make_interval raises) fails the advance instead of expiring every
 * retained snapshot. The live classes cannot tell the two clocks apart:
 * the pod and the engine share one in CI.
 */
class HogliteExpiryTest {
    private val json = jacksonObjectMapper()

    /** One statement the store sent: its SQL and its bound args (numbers as Long). */
    private class Sent(
        val sql: String,
        val args: List<Any?>,
    )

    /** Every `/query`, in order. */
    private val queries = CopyOnWriteArrayList<Sent>()

    /** Every write `/tx`, as its statements in order (fence included). */
    private val writes = CopyOnWriteArrayList<List<Sent>>()

    /** The meta row's snapshot_retention_seconds. */
    @Volatile private var retention: Long = 86_400

    /** What the cutoff read answers: the engine's cutoff (null past its calendar) and the floor candidate. */
    @Volatile private var cutoffRow: List<Any?> = listOf("2026-11-03T12:00:00.000Z", 5L)

    /** Whether one ended file is eligible for the purge, and every call after its purge `tx` answers 401. */
    @Volatile private var lapseAfterPurge = false

    @Volatile private var lapsed = false

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val body = ex.requestBody.readBytes().takeIf { it.isNotEmpty() }?.let { json.readTree(it) }
                if (lapsed) {
                    val refused = """{"code":"refused","error":"request not authorized"}""".toByteArray()
                    ex.sendResponseHeaders(401, refused.size.toLong())
                    ex.responseBody.use { it.write(refused) }
                    return@createContext
                }
                val answer: Any =
                    when {
                        body?.has("sql") == true -> {
                            val sent = sent(body)
                            queries += sent
                            result(rows = query(sent))
                        }
                        body?.has("statements") == true -> {
                            val statements = body["statements"].map { sent(it) }
                            writes += statements
                            if (lapseAfterPurge && statements.any { it.sql.startsWith("DELETE FROM files") }) {
                                lapsed = true
                            }
                            mapOf(
                                "results" to
                                    statements.map { st ->
                                        // Every statement reads back one 0 (the release
                                        // count, the guards' scalars); the snapshot DELETE
                                        // reports the rows the sweep expired.
                                        result(
                                            rows = listOf(listOf(0)),
                                            affected = if (st.sql.startsWith("DELETE FROM snapshots")) 3 else 0,
                                        )
                                    },
                            )
                        }
                        else -> mapOf("ok" to true)
                    }
                val bytes = json.writeValueAsBytes(answer)
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            start()
        }

    private val client = HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {})
    private val store = HogliteCatalogStore(client, { "t-$it" })
    private val expiry = HogliteExpiry(store, purgePage = 100, purgeBudgetMs = 10_000)

    @AfterEach
    fun stop() {
        client.close()
        server.stop(0)
    }

    private fun sent(node: JsonNode) =
        Sent(
            node["sql"].asText(),
            node["args"]?.map { json.treeToValue(it, Any::class.java) }?.map { if (it is Number) it.toLong() else it }
                .orEmpty(),
        )

    private fun result(
        rows: List<List<Any?>>,
        affected: Long = 0,
    ) = mapOf("cols" to List(rows.firstOrNull()?.size ?: 0) { "c$it" }, "rows" to rows, "affected" to affected)

    /** The tenant: head 9, floor 2, retention [retention], no consumer floor, ended file 7 under [lapseAfterPurge]. */
    private fun query(sent: Sent): List<List<Any?>> =
        when {
            sent.sql == store.metaSql -> listOf(listOf("wh", "s3://b/wh", 9, 1, 2, null, retention, 0))
            sent.sql.startsWith("SELECT c, (SELECT MIN(id) FROM snapshots") -> listOf(cutoffRow)
            lapseAfterPurge && sent.sql.startsWith("SELECT id, path FROM files") ->
                listOf(
                    listOf(7, "s3://b/f.parquet"),
                )
            else -> emptyList()
        }

    private val cutoffSql =
        "SELECT c, (SELECT MIN(id) FROM snapshots WHERE id >= ? AND ts >= c) " +
            "FROM (SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ? || ' seconds') AS c)"

    private fun cutoffRead() = queries.single { it.sql.startsWith("SELECT c, ") }

    private fun snapshotDelete() = writes.flatten().filter { it.sql.startsWith("DELETE FROM snapshots") }

    /**
     * The floor candidate and the cutoff it was compared with come from one
     * read, with the floor and the retention as the only bindings: this JVM
     * formats no time into the predicate, so a pod's clock skew cannot move
     * the cutoff (ExpiryService binds `:retention` the same way).
     */
    @Test
    fun `the retention cutoff is the engine's clock, read with the floor candidate it bounded`() {
        val result = expiry.runOnce("wh", 100)

        val read = cutoffRead()
        assertThat(read.sql).isEqualTo(cutoffSql)
        assertThat(read.args).containsExactly(2L, 86_400L)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        assertThat(result.snapshotsExpired).isEqualTo(3)
        assertThat(snapshotDelete().single().args).containsExactly(2L, 5L)
    }

    /**
     * SQLite answers a date outside its calendar with NULL, not an error,
     * and `ts >= NULL` matches nothing, so an unguarded sweep read "every
     * snapshot is stale" and expired the catalog up to head. The advance
     * fails the way PostgreSQL's make_interval fails it, through the
     * partial-result path the ledger records, and the floor does not move.
     */
    @Test
    fun `a retention outside the engine's calendar fails the advance instead of expiring everything`() {
        retention = Long.MAX_VALUE
        cutoffRow = listOf(null, null)

        val failure = catchThrowable { expiry.runOnce("wh", 100) }

        assertThat(failure).isInstanceOf(PartialResult::class.java)
        assertThat(failure.cause)
            .isInstanceOf(DateTimeException::class.java)
            .hasMessageContaining("snapshot_retention_seconds=${Long.MAX_VALUE}")
        val partial = (failure as PartialResult).partial as ExpiryResult
        assertThat(partial.snapshotsExpired).isZero()
        assertThat(partial.newEarliestSnapshotId).isEqualTo(2)
        assertThat(cutoffRead().args).containsExactly(2L, Long.MAX_VALUE)
        assertThat(snapshotDelete()).isEmpty()
        assertThat(writes.flatten().map { it.sql }).noneMatch { it.startsWith("UPDATE meta SET earliest_snapshot_id") }
        val ledger = writes.flatten().single { it.sql.startsWith("INSERT INTO maintenance_runs") }
        assertThat(ledger.args[4]).isEqualTo("failed")
    }

    /**
     * A NULL floor candidate under a real cutoff is the ordinary answer when
     * every retained snapshot is older than the window: the sweep advances
     * to head (within the batch), as before. Only a NULL cutoff refuses.
     */
    @Test
    fun `every snapshot older than a representable cutoff expires up to head`() {
        cutoffRow = listOf("2026-11-03T12:00:00.000Z", null)

        val result = expiry.runOnce("wh", 100)

        assertThat(result.newEarliestSnapshotId).isEqualTo(9)
        assertThat(snapshotDelete().single().args).containsExactly(2L, 9L)
    }

    /**
     * A floor advance that failed, then a token that lapsed after a purge
     * page applied (#680): the lapse fails the run, and the advance's own
     * failure is still counted, audited and carried as suppressed.
     */
    @Test
    fun `a token lapse in the purge keeps a failed floor advance on the record`() {
        retention = Long.MAX_VALUE
        cutoffRow = listOf(null, null)
        lapseAfterPurge = true
        val registry = SimpleMeterRegistry()
        Metrics.bind(registry)
        try {
            val bound = HogliteCatalogStore(client.forRequest("synthetic-expiring"), { "t-$it" })
            // A full page of one, so the purge reads another page after the one that applied.
            val selected = HogliteExpiry(bound, purgePage = 1, purgeBudgetMs = 10_000)

            val failure = catchThrowable { selected.runOnce("wh", 100) }

            assertThat(failure).isExactlyInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            val unknown = failure as HoglakeException.CommitOutcomeUnknown
            assertThat(unknown.tokenLapsed).isTrue()
            assertThat(unknown.applied).isTrue()
            assertThat(unknown.txid).startsWith("expiry-purge-data")
            assertThat(unknown.suppressed).singleElement().isInstanceOf(DateTimeException::class.java)
            assertThat(
                registry.find("hoglake_expiry_advance_failures_total").tag("catalog", "wh").counter()?.count(),
            ).isEqualTo(1.0)
        } finally {
            Metrics.clear()
        }
    }
}
