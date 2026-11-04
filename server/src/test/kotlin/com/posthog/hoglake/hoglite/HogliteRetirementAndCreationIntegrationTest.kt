package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.ReplacementTarget
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
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

/** Real published engine and MinIO; no synthetic successful maintenance receipts. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteRetirementAndCreationIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private val store get() = engine.store

    @AfterAll
    fun close() = engine.close()

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

    @Test
    fun `drained drop history cannot starve a later retirement with vectors`() {
        val cat = catalog()
        val seed = store.createTable(cat, "ns", "seed", listOf(ColumnDef("id", ColType.LONG)))
        val drop = store.dropTable(cat, "ns", "seed", seed.tableUuid, "test", "seed").snapshotId
        // SQL seeds only valid, already drained history. All target work below uses the real API.
        // 1001 older identities exceed the 1000-candidate page, without 1001 HTTP DDLs.
        write(
            cat,
            """WITH RECURSIVE n(i) AS (VALUES(2) UNION ALL SELECT i + 1 FROM n WHERE i < 1002)
            INSERT INTO tables(id, namespace_id, namespace, name, table_uuid,
                               next_field_id, created_snapshot, dropped_snapshot)
            SELECT i, namespace_id, namespace, 'old' || i, '00000000-0000-0000-0000-' || printf('%012d', i),
                   next_field_id, created_snapshot, dropped_snapshot FROM n CROSS JOIN tables WHERE tables.id = ?""",
            seed.tableId,
        )
        write(
            cat,
            """INSERT INTO table_versions(table_id, namespace_id, namespace, name, begin_snapshot, end_snapshot)
            SELECT id, namespace_id, namespace, name, created_snapshot, dropped_snapshot FROM tables WHERE id > 1""",
        )
        write(
            cat,
            """INSERT INTO columns(table_id, field_id, ordinal, name, type, nullable, begin_snapshot, end_snapshot)
            SELECT id, 1, 0, 'id', 'long', 1, created_snapshot, dropped_snapshot FROM tables WHERE id > 1""",
        )
        write(
            cat,
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "SELECT created_snapshot, 'table_created', id FROM tables WHERE id > 1",
        )
        write(
            cat,
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "SELECT dropped_snapshot, 'table_dropped', id FROM tables WHERE id > 1",
        )
        write(
            cat,
            """INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start,
            stats_state, begin_snapshot, end_snapshot)
            SELECT id, id, 's3://hoglake-it/$cat/ended-' || id, 1, 100, 0, 'pending',
                   created_snapshot, dropped_snapshot FROM tables""",
        )
        write(cat, "INSERT INTO table_totals(table_id) SELECT id FROM tables WHERE id > 1")
        write(cat, "UPDATE tables SET next_row_id = 1")
        write(cat, "UPDATE meta SET next_file_id = 1003, next_table_id = 1003 WHERE id = 1")
        val target = store.createTable(cat, "ns", "target", listOf(ColumnDef("id", ColType.LONG)))
        val path = "s3://hoglake-it/$cat/target.parquet"
        store.commit(
            cat,
            CommitRequest(appends = listOf(TableAppend("ns", "target", listOf(FileRegistration(path, 10, 100))))),
        )
        val fileId = store.listFiles(cat, "ns", "target").single().dataFileId
        val vectors = (1..2).map { "s3://hoglake-it/$cat/vector$it.puffin" }
        for ((i, vector) in vectors.withIndex()) {
            store.commit(
                cat,
                CommitRequest(
                    readSnapshot = store.getCatalog(cat).headSnapshotId,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "target",
                                listOf(DeleteFileRegistration(fileId, vector, (i + 1).toLong(), 20)),
                            ),
                        ),
                ),
            )
        }
        val targetDrop = store.dropTable(cat, "ns", "target", target.tableUuid, "test", "retire").snapshotId
        // Above-floor drops must not be touched, even with live files.
        val future = store.createTable(cat, "ns", "future", listOf(ColumnDef("id", ColType.LONG)))
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "future",
                            listOf(FileRegistration("s3://hoglake-it/$cat/future", 1, 100)),
                        ),
                    ),
            ),
        )
        store.dropTable(cat, "ns", "future", future.tableUuid, "test", "not yet")
        write(cat, "UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1", targetDrop)
        assertThat(
            query(cat, "SELECT COUNT(*) FROM tables WHERE dropped_snapshot <= ?", drop).firstRow()!![0].asLong(),
        ).isEqualTo(1002)
        val paused = store.catalogOperation(cat) { HogliteRetirement(store, runBudgetMs = 0).runOnce(cat) }
        assertThat(paused.rowsRetired).isZero()
        assertThat(paused.tablesRemaining).isEqualTo(1)
        assertLedger(cat, paused)
        val result = store.catalogOperation(cat) { HogliteRetirement(store).runOnce(cat) }
        assertThat(result.tables).isEqualTo(1)
        assertThat(result.rowsRetired).isEqualTo(1)
        assertThat(result.dvsRetired).isEqualTo(2)
        assertThat(result.pathsQueued).isEqualTo(3)
        assertThat(result.tablesRemaining).isZero()
        assertLedger(cat, result)
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", target.tableId).rows).isEmpty()
        assertThat(query(cat, "SELECT delete_file_id FROM dvs WHERE table_id = ?", target.tableId).rows).isEmpty()
        assertThat(query(cat, "SELECT path FROM removals ORDER BY path").rows.map { it[0].asString() })
            .containsExactlyElementsOf((vectors + path).sorted())
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", future.tableId).rows).hasSize(1)
        val again = store.catalogOperation(cat) { HogliteRetirement(store).runOnce(cat) }
        assertThat(again.rowsRetired).isZero()
        assertThat(again.tablesRemaining).isZero()
        assertLedger(cat, again)
    }

    private fun assertLedger(
        cat: String,
        result: com.posthog.hoglake.model.RetirementResult,
    ) {
        val row =
            query(
                cat,
                "SELECT status, result_json FROM maintenance_runs WHERE task = 'retirement' ORDER BY id DESC LIMIT 1",
            )
                .rows.single()
        assertThat(row[0]).isEqualTo("ok")
        val mapper = com.posthog.hoglake.wireObjectMapper()
        assertThat(
            mapper.readTree(row[1].asString()),
        ).isEqualTo(mapper.readTree(mapper.writeValueAsString(result)))
    }

    @Test
    fun `retirement production eligibility statements use a table keyed file probe`() {
        val cat = catalog()
        val table = store.createTable(cat, "ns", "files", listOf(ColumnDef("id", ColType.LONG)))
        val dropped = store.dropTable(cat, "ns", "files", table.tableUuid, "test", "plan").snapshotId
        // 100001 metadata rows, mostly live. No stats payload: this probes eligibility,
        // not IO cost, production cardinality, retirement batching, or reader capacity.
        write(
            cat,
            """WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 100001)
            INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start,
                              stats_state, begin_snapshot, end_snapshot)
            SELECT i, ?, 's3://hoglake-it/$cat/file-' || i, 1, 100, i, 'pending', ?,
                   CASE WHEN i > 96000 THEN ? ELSE NULL END FROM n""",
            table.tableId,
            table.readSnapshotId,
            dropped,
        )
        for (sql in listOf(HogliteRetirement.CANDIDATES_SQL, HogliteRetirement.REMAINING_SQL)) {
            val plan = query(cat, "EXPLAIN QUERY PLAN $sql", dropped).rows.joinToString { it.last().asString() }
            println("retirement eligibility: 100001 file rows; $sql; $plan")
            assertThat(plan).containsPattern("SEARCH f (?:EXISTS )?USING INDEX files_(?:table|scan) \\(table_id=\\?\\)")
                .doesNotContain("SCAN f").doesNotContain("TEMP B-TREE")
        }
        assertThat(query(cat, HogliteRetirement.CANDIDATES_SQL, dropped).rows.map { it[0].asLong() })
            .containsExactly(table.tableId)
        assertThat(query(cat, HogliteRetirement.REMAINING_SQL, dropped).firstRow()!![0].asLong()).isEqualTo(1)
    }

    private fun assertEndedVersions(
        cat: String,
        id: Long,
        renamed: Long,
        at: Long,
    ) {
        val rows = query(cat, "SELECT end_snapshot FROM columns WHERE table_id = ? ORDER BY begin_snapshot", id).rows
        assertThat(rows.map { it[0].asLongOrNull() }).containsExactly(renamed, at)
        assertThat(
            query(cat, "SELECT field_id FROM columns WHERE table_id = ? AND end_snapshot IS NULL", id).rows,
        ).isEmpty()
    }

    @Test
    fun `drop ends columns at its actual snapshot and expiry reclaims churn`() {
        val cat = catalog()
        val survivor = store.createTable(cat, "ns", "survivor", listOf(ColumnDef("kept", ColType.LONG)))
        repeat(12) { i ->
            val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            val renamed =
                HogliteAlters(store).alterTable(
                    cat,
                    "ns",
                    "t",
                    listOf(com.posthog.hoglake.model.AlterOp.RenameColumn("id", "renamed")),
                ).readSnapshotId
            store.createNamespace(cat, "gap$i") // The drop must use its allocated snapshot, not a pre-read head.
            val before = store.getCatalog(cat).headSnapshotId
            val dropped = store.dropTable(cat, "ns", "t", table.tableUuid, "test", "drop").snapshotId
            assertThat(dropped).isGreaterThan(before)
            assertThat(
                store.getTable(cat, "ns", "t", snapshot = before).columns.map { it.def.name },
            ).containsExactly("renamed")
            assertThatThrownBy { store.getTable(cat, "ns", "t", snapshot = dropped) }
                .isInstanceOf(com.posthog.hoglake.model.HoglakeException.NotFound::class.java)
            assertEndedVersions(cat, table.tableId, renamed, dropped)
        }
        expireAll(cat)
        assertThat(query(cat, "SELECT table_id FROM columns").rows.map { it[0].asLong() })
            .containsExactly(survivor.tableId)
        assertThat(store.getTable(cat, "ns", "survivor").columns.map { it.def.name }).containsExactly("kept")
    }

    @Test
    fun `replacement ends only old columns at the publication snapshot and expiry reclaims churn`() {
        val cat = catalog()
        val survivor = store.createTable(cat, "ns", "survivor", listOf(ColumnDef("kept", ColType.LONG)))
        var current = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        repeat(12) { i ->
            val renamed =
                HogliteAlters(store).alterTable(
                    cat,
                    "ns",
                    "t",
                    listOf(
                        com.posthog.hoglake.model.AlterOp.RenameColumn(current.columns.single().def.name, "renamed$i"),
                    ),
                ).readSnapshotId
            val before = store.getCatalog(cat).headSnapshotId
            val op = UUID.randomUUID()
            store.tableCreations.prepare(
                cat,
                op,
                TableCreationDefinition(
                    "ns",
                    "t",
                    listOf(ColumnDef("new$i", ColType.LONG)),
                    replacement = ReplacementTarget(current.tableUuid, before),
                    author = "test",
                    message = "replace",
                ),
            )
            store.createNamespace(cat, "gap$i")
            val published = store.tableCreations.publish(cat, op, emptyList())
            assertThat(published.state).isEqualTo("committed")
            val at = published.snapshotId!!
            assertThat(at).isGreaterThan(before)
            assertThat(store.getTable(cat, "ns", "t", snapshot = before).tableUuid).isEqualTo(current.tableUuid)
            assertEndedVersions(cat, current.tableId, renamed, at)
            current = store.getTable(cat, "ns", "t", snapshot = at)
            assertThat(current.columns.map { it.def.name }).containsExactly("new$i")
            assertThat(
                query(cat, "SELECT end_snapshot FROM columns WHERE table_id = ?", current.tableId).rows.single()[0],
            ).isNull()
        }
        expireAll(cat)
        assertThat(
            query(cat, "SELECT table_id FROM columns").rows.map { it[0].asLong() },
        ).containsExactlyInAnyOrder(survivor.tableId, current.tableId)
        assertThat(store.getTable(cat, "ns", "survivor").columns.map { it.def.name }).containsExactly("kept")
    }

    private fun expireAll(cat: String) {
        // Backdate real snapshots, then drive actual expiry without wall-clock sleeps.
        write(cat, "UPDATE meta SET snapshot_retention_seconds = 1 WHERE id = 1")
        write(cat, "UPDATE snapshots SET ts = '2000-01-01T00:00:00.000Z'")
        val result = store.catalogOperation(cat) { HogliteExpiry(store, 100, 10000).runOnce(cat, 1000) }
        assertThat(result.newEarliestSnapshotId).isEqualTo(store.getCatalog(cat).headSnapshotId)
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
                val racing = HogliteCatalogStore(client)
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
    fun `ordinary commit invalid footer is a typed HTTP refusal and valid boundaries publish`() {
        val cat = catalog()
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val cfg =
            engine.config().copy(
                database = com.posthog.hoglake.DatabaseBackend.HOGLITE,
                hogliteUrl = engine.url(),
                hydratorIntervalMs = 0,
                expiryIntervalMs = 0,
                cleanupIntervalMs = 0,
                compactionIntervalMs = 0,
                retirementIntervalMs = 0,
            )
        val app = App.buildHoglite(cfg, store, HealthProbe.forProbe(1000) { engine.client.reachable() })
        testApplication {
            application { app.module(this) }
            val before = store.getCatalog(cat).headSnapshotId
            for ((i, footer) in listOf(-1, 93, 92, 0).withIndex()) {
                val response =
                    client.post("/v1/catalogs/$cat/commit") {
                        contentType(ContentType.Application.Json)
                        setBody(
                            """{"appends":[{"namespace":"ns","table":"t","files":[{
                                "path":"s3://hoglake-it/$cat/http$i","record_count":1,
                                "file_size_bytes":100,"footer_size":$footer}]}]}""",
                        )
                    }
                if (footer < 0 || footer > 92) {
                    assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    val error = jacksonObjectMapper().readTree(response.bodyAsText())
                    assertThat(error["error"].asText()).isEqualTo("validation")
                    assertThat(error["detail"].asText()).isEqualTo("invalid footer_size")
                    assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
                } else {
                    assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                }
            }
            assertThat(store.listFiles(cat, "ns", "t")).hasSize(2)
            val dataId = store.listFiles(cat, "ns", "t").first().dataFileId
            for (count in listOf(0, -1, 1)) {
                val beforeDelete = store.getCatalog(cat).headSnapshotId
                val response =
                    client.post("/v1/catalogs/$cat/commit") {
                        contentType(ContentType.Application.Json)
                        setBody(
                            """{"read_snapshot":$beforeDelete,"deletes":[{"namespace":"ns","table":"t","files":[{
                                "data_file_id":$dataId,"path":"s3://hoglake-it/$cat/dv$count",
                                "delete_count":$count,"file_size_bytes":20}]}]}""",
                        )
                    }
                if (count <= 0) {
                    assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    val error = jacksonObjectMapper().readTree(response.bodyAsText())
                    assertThat(error["error"].asText()).isEqualTo("validation")
                    assertThat(
                        error["detail"].asText(),
                    ).isEqualTo("delete_count must be > 0 for data_file_id $dataId in ns.t, got $count")
                    assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(beforeDelete)
                } else {
                    assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                }
            }
        }
    }
}
