package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Queued publication cannot replace a read-only expired-aborted receipt. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCreationExpiryIntegrationTest {
    private var started = false
    private val engine by lazy { HogliteConformanceEngine().also { started = true } }
    private val store get() = engine.store
    private val json = jacksonObjectMapper()

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    @ParameterizedTest
    @ValueSource(strings = ["2000-01-01T00:00:00Z", "2000-01-01T00:00:00.000Z", "2000-01-01T00:00:00.000000000Z"])
    fun `expiry boundaries parse all existing timestamp spellings`(deadline: String) {
        val catalog = HogliteConformanceEngine.uniqueName("dates")
        val tenant = HogliteConformanceEngine.TENANTS.tenantFor(catalog)
        store.createCatalog(catalog, "s3://hoglake-it/$catalog")
        store.createNamespace(catalog, "ns")
        val operation = UUID.randomUUID()
        store.tableCreations.prepare(
            catalog,
            operation,
            TableCreationDefinition("ns", "pending", listOf(ColumnDef("id", ColType.LONG))),
        )
        engine.client.tx(
            tenant,
            "date-$operation",
            null,
            listOf(
                HogliteStatement(
                    "UPDATE table_creations SET expires_at = ? WHERE operation = ?",
                    listOf(deadline, operation.toString()),
                ),
            ),
        )
        for ((cut, expired) in listOf(
            "1999-12-31T23:59:59.999Z" to 0L,
            "2000-01-01T00:00:00.000Z" to 1L,
            "2000-01-01T00:00:00.001000000Z" to 1L,
        )) {
            val row =
                engine.client.query(
                    tenant,
                    "SELECT CASE WHEN julianday(expires_at) <= julianday(?) THEN 1 ELSE 0 END, " +
                        "CASE WHEN julianday(expires_at) > julianday(?) THEN 1 ELSE 0 END " +
                        "FROM table_creations WHERE operation = ?",
                    listOf(cut, cut, operation.toString()),
                ).rows.single()
            assertThat(row[0].asLong()).isEqualTo(expired)
            assertThat(row[1].asLong()).isEqualTo(1L - expired)
        }
        assertThat(store.tableCreations.status(catalog, operation).reason).isEqualTo("expired")
        assertThat(engine.client.query(tenant, "SELECT state FROM table_creations").rows.single().single())
            .isEqualTo("prepared")
        assertThat(store.tableCreations.publish(catalog, operation, emptyList()).reason).isEqualTo("expired")
    }

    @Test
    fun `expiry predicates retain primary key lookup with one hundred thousand receipts`() {
        val catalog = HogliteConformanceEngine.uniqueName("plans")
        val tenant = HogliteConformanceEngine.TENANTS.tenantFor(catalog)
        store.createCatalog(catalog, "s3://hoglake-it/$catalog")
        engine.client.tx(
            tenant,
            "scale-$catalog",
            null,
            listOf(
                HogliteStatement(
                    "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100000) " +
                        "INSERT INTO table_creations(operation, namespace_id, definition_json, table_uuid, " +
                        "write_path, state, expires_at, created_at) " +
                        "SELECT printf('scale-%06d',i), 1, '{}', 'scale', 's3://bucket/', 'prepared', " +
                        "'2100-01-01T00:00:00Z', '2000-01-01T00:00:00Z' FROM n",
                ),
            ),
        )
        assertThat(engine.client.query(tenant, "SELECT COUNT(*) FROM table_creations").rows.single().single().asLong())
            .isEqualTo(100_000L)
        val predicates =
            listOf(
                "SELECT state FROM table_creations WHERE operation = ?",
                "UPDATE table_creations SET state = 'committed' WHERE operation = ? AND state = 'prepared' " +
                    "AND julianday(expires_at) > ${HogliteSchema.CREATION_NOW_SQL}",
                "UPDATE table_creations SET state = 'aborted' WHERE operation = ? AND state = 'prepared' " +
                    "AND julianday(expires_at) <= ${HogliteSchema.CREATION_NOW_SQL}",
            )
        val plans =
            engine.client.tx(
                tenant,
                "plans-$catalog",
                null,
                predicates.map { HogliteStatement("EXPLAIN QUERY PLAN $it", listOf("scale-050000")) },
            )
        for (plan in plans) {
            val detail = plan.rows.joinToString(" ") { it[3].asString() }
            assertThat(detail).contains("SEARCH", "INDEX", "operation=?").doesNotContain("SCAN")
        }
    }

    @ParameterizedTest
    @CsvSource(
        "publish,false", "publish,true",
        "reject-preflight,false", "reject-preflight,true",
        "reject-definition,false", "reject-definition,true",
        "reject-race,false", "reject-race,true",
        "abort,false",
    )
    fun `storage-time expiry wins every terminal transition`(
        action: String,
        bypassInitialGuard: Boolean,
    ) {
        val catalog = HogliteConformanceEngine.uniqueName("expiry")
        val tenant = HogliteConformanceEngine.TENANTS.tenantFor(catalog)
        store.createCatalog(catalog, "s3://hoglake-it/$catalog")
        store.createNamespace(catalog, "ns")
        val operation = UUID.randomUUID()
        val definition = TableCreationDefinition("ns", "pending", listOf(ColumnDef("id", ColType.LONG)))
        store.tableCreations.prepare(catalog, operation, definition)
        if (action == "reject-preflight") store.dropNamespace(catalog, "ns", author = "fixture", message = "drop")
        if (action == "reject-definition") {
            engine.client.tx(
                tenant,
                "invalid-$operation",
                null,
                listOf(
                    HogliteStatement(
                        "UPDATE table_creations SET definition_json = ? WHERE operation = ?",
                        listOf(
                            TableCreationDefinitionCodec.encode(
                                definition.copy(properties = mapOf("hoglake.reserved" to "invalid")),
                            ),
                            operation.toString(),
                        ),
                    ),
                ),
            )
        }
        val entered = CountDownLatch(1)
        val resume = CountDownLatch(1)
        val paused = AtomicBoolean()
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val http = HttpClient.newHttpClient()
        proxy.executor = executor
        proxy.createContext("/") { exchange ->
            try {
                var body = exchange.requestBody.readBytes()
                val text = body.decodeToString()
                val transition = if (action == "abort") "client_abort" in text else "SET files_json" in text
                if (exchange.requestURI.path.endsWith("/tx") && transition) {
                    if (paused.compareAndSet(false, true)) {
                        entered.countDown()
                        check(resume.await(20, TimeUnit.SECONDS))
                    }
                    if (bypassInitialGuard) {
                        // Isolate the final storage guard from the earlier defensive guard.
                        val request = json.readTree(body)
                        for (statement in request["statements"] as ArrayNode) {
                            val sql = statement["sql"].asText()
                            if ("UPDATE table_creations SET files_json" in sql) {
                                (statement as ObjectNode).put(
                                    "sql",
                                    sql.replace(" AND julianday(expires_at) > ${HogliteSchema.CREATION_NOW_SQL}", ""),
                                )
                            }
                        }
                        body = json.writeValueAsBytes(request)
                    }
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
                val result =
                    executor.submit<com.posthog.hoglake.service.TableCreation> {
                        if (action == "abort") {
                            racing.tableCreations.abort(catalog, operation)
                        } else {
                            racing.tableCreations.publish(catalog, operation, emptyList())
                        }
                    }
                assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue()
                if (action == "reject-race") store.dropNamespace(catalog, "ns", author = "fixture", message = "drop")
                // Expire after request construction, not before its application timestamp could be captured.
                engine.client.tx(
                    tenant,
                    "expire-$operation",
                    null,
                    listOf(
                        HogliteStatement(
                            "UPDATE table_creations SET expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') " +
                                "WHERE operation = ?",
                            listOf(operation.toString()),
                        ),
                    ),
                )
                val head = store.getCatalog(catalog).headSnapshotId
                val snapshots = engine.client.query(tenant, "SELECT COUNT(*) FROM snapshots").rows.single().single()
                val status = store.tableCreations.status(catalog, operation)
                assertThat(status.state).isEqualTo("aborted")
                assertThat(status.reason).isEqualTo("expired")
                assertThat(
                    engine.client.query(
                        tenant,
                        "SELECT state FROM table_creations WHERE operation = ?",
                        listOf(operation.toString()),
                    )
                        .rows.single().single(),
                ).isEqualTo("prepared")
                resume.countDown()
                val completed = result.get(25, TimeUnit.SECONDS)
                assertThat(completed.state).isEqualTo("aborted")
                assertThat(completed.reason).isEqualTo("expired")
                assertThat(store.tableCreations.status(catalog, operation).state).isEqualTo("aborted")
                assertThat(store.getCatalog(catalog).headSnapshotId).isEqualTo(head)
                assertThat(engine.client.query(tenant, "SELECT COUNT(*) FROM snapshots").rows.single().single())
                    .isEqualTo(snapshots)
                for (table in listOf("tables", "table_versions", "columns", "files")) {
                    assertThat(
                        engine.client.query(tenant, "SELECT COUNT(*) FROM $table").rows.single().single().asLong(),
                    )
                        .isEqualTo(0L)
                }
                val receipt =
                    engine.client.query(
                        tenant,
                        "SELECT state, reason, snapshot_id, schema_version, files_json " +
                            "FROM table_creations WHERE operation = ?",
                        listOf(operation.toString()),
                    ).rows.single()
                assertThat(receipt).containsExactly("aborted", "expired", null, null, null)
            }
        } finally {
            resume.countDown()
            proxy.stop(0)
            executor.close()
        }
    }
}
