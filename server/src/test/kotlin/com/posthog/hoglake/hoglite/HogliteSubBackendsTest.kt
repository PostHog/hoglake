package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** Check adapter guards and bindings without an engine or a catalog registry. */
class HogliteSubBackendsTest {
    private val json = jacksonObjectMapper()
    private val owner = UUID.randomUUID()
    private val upload = UUID.randomUUID()
    private val view = UUID.randomUUID()
    private val definition = TableCreationDefinition("ns", "t", listOf(ColumnDef("x", ColType.LONG)))
    private val paths = CopyOnWriteArrayList<String>()
    private val statements = CopyOnWriteArrayList<JsonNode>()
    private val resolutions = AtomicInteger()
    private var storedOwner = owner
    private val server =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                paths += ex.requestURI.path
                val body = json.readTree(ex.requestBody.readBytes())

                fun result(
                    rows: List<List<Any?>> = emptyList(),
                    affected: Int = 0,
                ): Map<String, Any> =
                    mapOf(
                        "cols" to List(rows.firstOrNull()?.size ?: 0) { "c$it" },
                        "rows" to rows,
                        "affected" to affected,
                    )
                val answer =
                    if (body.has("statements")) {
                        val results =
                            body["statements"].map { stmt ->
                                statements.add(stmt)
                                if (stmt["sql"].asText().startsWith("SELECT (SELECT next_snapshot")) {
                                    result(listOf(listOf(8, 3, 1, 1)))
                                } else {
                                    result(affected = 1)
                                }
                            }
                        mapOf("results" to results)
                    } else {
                        val sql = body["sql"].asText()
                        when {
                            sql.startsWith("SELECT name, data_path,") ->
                                result(listOf(listOf("cat", "s3://bucket/cat", 7, 0, 0, null, null, 0)))
                            sql.startsWith("SELECT table_uuid, definition_json") ->
                                result(
                                    listOf(
                                        listOf(
                                            view.toString(),
                                            TableCreationDefinitionCodec.encode(definition),
                                            "s3://bucket/cat/data/$view/",
                                            "prepared",
                                            Instant.now().plusSeconds(3600).toString(),
                                            null,
                                            null,
                                            null,
                                        ),
                                    ),
                                )
                            sql.startsWith("SELECT upload_id, owner") ->
                                result(
                                    listOf(
                                        listOf(
                                            upload,
                                            storedOwner,
                                            "s3://bucket/cat",
                                            "s3://bucket/cat/f",
                                            "data",
                                            "active",
                                            Instant.now().plusSeconds(3600).toString(),
                                        ),
                                    ),
                                )
                            sql.startsWith(
                                "SELECT id, name, contained_field",
                            ) -> result(listOf(listOf(1, "ns", null, null)))
                            sql.startsWith(
                                "SELECT id, view_uuid, name",
                            ) -> result(listOf(listOf(1, view, "v", "trino", "SELECT 1")))
                            sql.startsWith(
                                "SELECT id, view_uuid, dialect",
                            ) -> result(listOf(listOf(1, view, "trino", "SELECT 1")))
                            else -> result()
                        }
                    }
                val bytes = json.writeValueAsBytes(answer)
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            start()
        }
    private val client = HogliteClient("http://127.0.0.1:${server.address.port}")
    private val store =
        HogliteCatalogStore(client, {
            resolutions.incrementAndGet()
            "opaque_1"
        })

    @AfterEach
    fun stop() {
        client.close()
        server.stop(0)
    }

    @Test
    fun `claim keeps upload identity and one tenant binding through its readback`() {
        val claim = store.uploads.claim("cat", upload, owner, "s3://bucket/cat/", "data")
        assertThat(claim.owner).isEqualTo(owner)
        assertThat(resolutions.get()).isEqualTo(1)
        assertThat(paths).allMatch { it.startsWith("/v1/t/opaque_1/") }
        assertThat(statements.map { it["sql"].asText() }).anyMatch { it.startsWith("INSERT OR IGNORE INTO uploads") }
        storedOwner = UUID.randomUUID()
        assertThatThrownBy { store.uploads.claim("cat", upload, owner, "s3://bucket/cat", "data") }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
    }

    @Test
    fun `renewal and abandonment preserve the active owner fence`() {
        assertThat(store.uploads.renew("cat", owner)).isEqualTo(1)
        assertThat(store.uploads.abandon("cat", owner, listOf("s3://bucket/cat/f"))).isEqualTo(1)
        val updates = statements.filter { it["sql"].asText().startsWith("UPDATE uploads") }
        assertThat(updates).hasSize(2)
        assertThat(
            updates.map { it["sql"].asText() },
        ).allMatch { it.contains("state = 'active'") && it.contains("owner = ?") }
        assertThat(updates.map { it["args"].toString() }).allMatch { it.contains(owner.toString()) }
    }

    @Test
    fun `upload prefixes cannot escape the catalog root`() {
        assertThatThrownBy { store.uploads.claim("cat", upload, owner, "s3://bucket/other", "data") }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(statements).isEmpty()
    }

    @Test
    fun `view create fences namespace and duplicate name in the write transaction`() {
        val created = store.views.create("cat", "ns", "v", "SELECT 1")
        assertThat(created.viewId).isEqualTo(3)
        assertThat(resolutions.get()).isEqualTo(1)
        val guards = statements.filter { it.has("expect_scalar_int") }
        assertThat(
            guards.map {
                it["sql"].asText()
            },
        ).anyMatch { it.contains("namespaces WHERE id = ? AND dropped_snapshot IS NULL") }
        assertThat(statements.map { it["sql"].asText() }).anyMatch { it.contains("'view_created'") }
    }

    @Test
    fun `view get list and drop use the selected tenant and record the DDL change`() {
        assertThat(store.views.get("cat", "ns", "v").viewUuid).isEqualTo(view)
        assertThat(store.views.list("cat", "ns").single().name).isEqualTo("v")
        assertThat(store.views.drop("cat", "ns", "v", author = "test", message = "drop view").snapshotId).isEqualTo(8)
        assertThat(statements.map { it["sql"].asText() }).anyMatch { it.contains("'view_dropped'") }
        assertThat(paths).allMatch { it.startsWith("/v1/t/opaque_1/") }
    }

    @Test
    fun `creation status applies guarded lazy expiry before reading the durable receipt`() {
        val receipt = store.tableCreations.status("cat", upload)
        assertThat(receipt.definition).isEqualTo(definition)
        assertThat(receipt.tableUuid).isEqualTo(view)
        assertThat(resolutions.get()).isEqualTo(1)
        assertThat(statements.map { it["sql"].asText() }).anyMatch {
            it.contains("state = 'prepared' AND expires_at <= ?") && it.contains("reason = 'expired'")
        }
    }

    @Test
    fun `alter and partition surfaces reject invalid inputs before transport`() {
        assertThatThrownBy { store.alters.alterTable("cat", "ns", "t", emptyList()) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { store.partitions.partitionStats("cat", null, "t", 10) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { store.partitions.listPartitions("cat", "ns", "t", offset = -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { store.partitionLookup.lookup("cat", "ns", "t", 0, emptyList()) }
            .isInstanceOf(HoglakeException.PartitionLookupInvalidRequest::class.java)
        assertThat(paths).isEmpty()
    }
}
