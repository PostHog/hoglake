package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.api.toDto
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileOrderingBounds
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

/** Small valid payloads: transport projection evidence, NOT physical I/O or capacity evidence. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteChangesProjectionIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private val json = jacksonObjectMapper()
    private val wire = wireObjectMapper()
    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null

    private fun engine() = live ?: HogliteConformanceEngine().also { live = it }

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    private val full =
        "f.id, f.table_id, f.path, f.file_format, f.record_count, f.file_size_bytes, f.footer_size, " +
            "f.row_id_start, f.explicit_row_ids, f.spec_id, f.partition_values_json, f.stats_state, " +
            "f.stats_json, f.split_offsets_json, f.begin_snapshot"
    private val projected =
        "f.id, f.table_id, f.path, f.file_format, f.record_count, f.file_size_bytes, f.footer_size, " +
            "f.row_id_start, f.explicit_row_ids, f.spec_id, f.partition_values_json, f.stats_state, " +
            "NULL AS stats_json, NULL AS split_offsets_json, f.begin_snapshot"
    private val suffix =
        " FROM files f WHERE f.table_id = ? AND f.begin_snapshot > ? AND f.begin_snapshot <= ? " +
            "AND NOT EXISTS (SELECT 1 FROM changes c WHERE c.snapshot_id = f.begin_snapshot " +
            "AND c.kind = 'table_compacted' AND c.object_id = f.table_id) ORDER BY f.begin_snapshot, f.id"

    private data class Read(val path: String, val request: JsonNode, val bytes: ByteArray, val status: Int)

    private inner class Capture(private val engine: HogliteConformanceEngine) : AutoCloseable {
        val reference = AtomicBoolean()
        val reads = CopyOnWriteArrayList<Read>()
        private val executor = Executors.newFixedThreadPool(2)
        private val http = HttpClient.newHttpClient()
        private val proxy =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                this.executor = this@Capture.executor
                createContext("/") { exchange ->
                    val raw = exchange.requestBody.readBytes()
                    val request = if (raw.isEmpty()) json.createObjectNode() else json.readTree(raw)
                    val sql = request["sql"]?.asText().orEmpty()
                    val feed = sql.contains("FROM files f WHERE f.table_id = ? AND f.begin_snapshot > ?")
                    val forwarded =
                        if (feed && reference.get()) {
                            (request.deepCopy<JsonNode>() as ObjectNode).apply {
                                put("sql", "SELECT $full" + sql.substring(sql.indexOf(" FROM files f")))
                            }.let(json::writeValueAsBytes)
                        } else {
                            raw
                        }
                    val response =
                        http.send(
                            HttpRequest.newBuilder(URI.create(engine.url() + exchange.requestURI))
                                .header("Content-Type", "application/json")
                                .method(exchange.requestMethod, HttpRequest.BodyPublishers.ofByteArray(forwarded))
                                .build(),
                            HttpResponse.BodyHandlers.ofByteArray(),
                        )
                    if (exchange.requestURI.path.endsWith("/query")) {
                        reads += Read(exchange.requestURI.path, request, response.body(), response.statusCode())
                    }
                    exchange.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                    exchange.responseBody.use { it.write(response.body()) }
                }
                start()
            }
        val client = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}")

        override fun close() {
            client.close()
            proxy.stop(0)
            executor.shutdownNow()
            http.close()
        }
    }

    private inner class Fixture(
        val backend: Backend,
        val store: CatalogStore,
        val app: App,
        val cat: String,
        val capture: Capture?,
    ) {
        val root = "/v1/catalogs/$cat/namespaces/ns/tables/t"

        fun alter(ops: List<AlterOp>) =
            if (backend == Backend.HOGLITE) {
                (store as HogliteCatalogStore).alters.alterTable(cat, "ns", "t", ops)
            } else {
                AlterService(pg!!.jdbi).alterTable(cat, "ns", "t", ops)
            }

        fun append(
            name: String,
            stats: Boolean = false,
            partition: List<String?>? = null,
        ): Long =
            store.commit(
                cat,
                CommitRequest(
                    readSnapshot = store.getCatalog(cat).headSnapshotId,
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(
                                    FileRegistration(
                                        "s3://hoglake-it/$cat/$name",
                                        7,
                                        101,
                                        if (stats) 11 else null,
                                        if (stats) {
                                            listOf(
                                                ColumnStats(
                                                    1,
                                                    7,
                                                    0,
                                                    null,
                                                    null,
                                                    ByteBuffer.allocate(
                                                        8,
                                                    ).order(ByteOrder.LITTLE_ENDIAN).putLong(1).array(),
                                                    ByteBuffer.allocate(
                                                        8,
                                                    ).order(ByteOrder.LITTLE_ENDIAN).putLong(7).array(),
                                                ),
                                            )
                                        } else {
                                            null
                                        },
                                        partition,
                                        if (stats) listOf(4L, 40L) else null,
                                    ),
                                ),
                            ),
                        ),
                ),
            ).snapshotId

        fun syntheticFlags() {
            // Raw flags represent legacy/hydrator states, NOT API-written stats or explicit-row-id proof.
            if (backend == Backend.HOGLITE) {
                val hg = store as HogliteCatalogStore
                hg.catalogOperation(cat) {
                    hg.runTx(
                        cat,
                        HogliteCatalogStore.freshTxid("projection-fixture"),
                        null,
                        HogliteTx().apply {
                            add("UPDATE files SET explicit_row_ids = 1 WHERE path LIKE '%pending'")
                            add("UPDATE files SET stats_state = 'failed' WHERE path LIKE '%failed'")
                        },
                    )
                }
            } else {
                pg!!.jdbi.useHandle<Exception> { h ->
                    h.execute(
                        "UPDATE hog_data_file SET explicit_row_ids = true WHERE catalog_id = " +
                            "(SELECT catalog_id FROM hog_catalog WHERE name = ?) AND path LIKE '%pending'",
                        cat,
                    )
                    h.execute(
                        "UPDATE hog_data_file SET stats_state = 'failed' WHERE catalog_id = " +
                            "(SELECT catalog_id FROM hog_catalog WHERE name = ?) AND path LIKE '%failed'",
                        cat,
                    )
                }
            }
        }
    }

    private fun fixture(
        backend: Backend,
        body: (Fixture) -> Unit,
    ) {
        val capture = if (backend == Backend.HOGLITE) Capture(engine()) else null
        try {
            val db = if (backend == Backend.POSTGRES) pg ?: PgTestSupport.freshDatabase().also { pg = it } else null
            val store =
                if (capture != null) {
                    HogliteCatalogStore(capture.client)
                } else {
                    PostgresCatalogStore(CatalogService(db!!.jdbi), CommitService(db.jdbi), ScanService(db.jdbi))
                }
            val app =
                if (capture != null) {
                    App.buildHoglite(
                        engine().config().copy(database = DatabaseBackend.HOGLITE, hogliteUrl = engine().url()),
                        store as HogliteCatalogStore,
                        HealthProbe.forProbe(1_000) { true },
                    )
                } else {
                    App.build(Config(dbPoolSize = 8), db!!.jdbi)
                }
            app.use {
                val cat = "projection" + UUID.randomUUID().toString().replace("-", "")
                store.createCatalog(cat, "s3://hoglake-it/$cat")
                store.createNamespace(cat, "ns")
                store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                body(Fixture(backend, store, app, cat, capture))
            }
        } finally {
            capture?.close()
        }
    }

    @ParameterizedTest(name = "projection public fields {0}")
    @EnumSource(Backend::class)
    fun publicFields(backend: Backend) =
        fixture(backend) { f ->
            val from = f.store.getCatalog(f.cat).headSnapshotId
            val first = f.append("pending")
            val second = f.append("failed")
            f.alter(listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))))
            val last = f.append("provided", stats = true, partition = listOf("7"))
            f.syntheticFlags()
            val table = f.store.getTable(f.cat, "ns", "t")
            val files = f.store.listFiles(f.cat, "ns", "t")
            val expected =
                listOf(
                    DataFile(
                        files[0].dataFileId, table.tableId, "s3://hoglake-it/${f.cat}/pending", "parquet", 7, 101,
                        null, 0, StatsState.PENDING, first, explicitRowIds = true,
                    ),
                    DataFile(
                        files[1].dataFileId, table.tableId, "s3://hoglake-it/${f.cat}/failed", "parquet", 7, 101,
                        null, 7, StatsState.FAILED, second,
                    ),
                    DataFile(
                        files[2].dataFileId, table.tableId, "s3://hoglake-it/${f.cat}/provided", "parquet", 7, 101,
                        11, 14, StatsState.PROVIDED, last, specId = 1, partitionValues = listOf("7"),
                    ),
                )
            val plan = f.store.changes(f.cat, "ns", "t", from, last)
            assertThat(plan.tableUuid).isEqualTo(table.tableUuid)
            assertThat(plan.fromSnapshot).isEqualTo(from)
            assertThat(plan.toSnapshot).isEqualTo(last)
            assertThat(plan.files).containsExactlyElementsOf(expected)
            assertThat(plan.deleteFiles).isEmpty()
            testApplication {
                application { f.app.module(this) }
                val url = "${f.root}/changes?from_snapshot=$from&to_snapshot=$last"
                val projectedResponse = client.get(url)
                assertThat(projectedResponse.status).isEqualTo(HttpStatusCode.OK)
                val publicBytes = projectedResponse.bodyAsText()
                assertThat(json.readTree(publicBytes)).isEqualTo(json.readTree(wire.writeValueAsBytes(plan.toDto())))
                if (f.capture != null) {
                    f.capture.reference.set(true)
                    val reference = client.get(url)
                    assertThat(reference.status).isEqualTo(HttpStatusCode.OK)
                    assertThat(reference.bodyAsText()).isEqualTo(publicBytes)
                }
            }
        }

    @ParameterizedTest(name = "projection retained history {0}")
    @EnumSource(Backend::class)
    fun retainedHistory(backend: Backend) =
        fixture(backend) { f ->
            val from = f.store.getCatalog(f.cat).headSnapshotId
            val appended = f.append("history", stats = true)
            val file = f.store.listFiles(f.cat, "ns", "t").single()

            fun dv(
                name: String,
                count: Long,
                read: Long,
            ) = f.store.commit(
                f.cat,
                CommitRequest(
                    readSnapshot = read,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        file.dataFileId,
                                        "s3://hoglake-it/${f.cat}/$name",
                                        count,
                                        59,
                                    ),
                                ),
                            ),
                        ),
                ),
            ).snapshotId
            val first = dv("first.puffin", 1, appended)
            val to = dv("second.puffin", 2, first)
            f.store.truncateTable(
                f.cat,
                "ns",
                "t",
                f.store.getTable(f.cat, "ns", "t").tableUuid,
                "test",
                "after window",
            )
            val plan = f.store.changes(f.cat, "ns", "t", from, to)
            assertThat(plan.files).containsExactly(file.copy(orderingBounds = null))
            assertThat(plan.deleteFiles.map { it.path }).containsExactly(
                "s3://hoglake-it/${f.cat}/first.puffin",
                "s3://hoglake-it/${f.cat}/second.puffin",
            )
            assertThat(plan.deleteFiles.map { it.beginSnapshot }).containsExactly(first, to)
            testApplication {
                application { f.app.module(this) }
                val url = "${f.root}/changes?from_snapshot=$from&to_snapshot=$to"
                val answer = client.get(url)
                assertThat(answer.status).isEqualTo(HttpStatusCode.OK)
                val body = answer.bodyAsText()
                assertThat(json.readTree(body)).isEqualTo(json.readTree(wire.writeValueAsBytes(plan.toDto())))
                f.capture?.reference?.set(true)
                assertThat(client.get(url).bodyAsText()).isEqualTo(body)
            }
        }

    @Test
    fun `hoglite raw changefeed preserves fifteen positions and omits payloads`() =
        fixture(Backend.HOGLITE) { f ->
            f.store.commit(
                f.cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/${f.cat}/earlier", 3, 89)),
                            ),
                        ),
                ),
            )
            val from = f.store.getCatalog(f.cat).headSnapshotId
            val to = f.append("raw", stats = true)
            val hg = f.store as HogliteCatalogStore
            val tenant = hg.catalogOperation(f.cat) { hg.boundTenant(f.cat) }
            val capture = f.capture!!
            // Warm schema probes only. The operation still performs its normal authoritative binding reads.
            hg.getCatalog(f.cat)
            capture.reads.clear()
            val outcome = runCatching { hg.changes(f.cat, "ns", "t", from, to) }
            val projectedTrace = capture.reads.toList()
            val read = projectedTrace.single { "f.begin_snapshot > ?" in it.request["sql"].asText() }
            assertThat(read.path).isEqualTo("/v1/t/$tenant/query")
            assertThat(read.status).isEqualTo(200)
            val row = json.readTree(read.bytes)["rows"].single()
            assertThat(row.size()).describedAs("file projection keeps all fifteen mapper positions").isEqualTo(15)
            assertThat(row[12].isNull).describedAs("stats payload slot12 must be NULL").isTrue()
            assertThat(row[13].isNull).describedAs("split payload slot13 must be NULL").isTrue()
            assertThat(row[14].asLong()).describedAs("snapshot remains at slot14").isEqualTo(to)
            val plan = outcome.getOrThrow()
            val file = plan.files.single()
            assertThat(file.beginSnapshot).describedAs("mapper reads snapshot from slot14, not another numeric cell")
                .isEqualTo(to)
            val expected =
                json.readTree(
                    json.writeValueAsBytes(
                        listOf(
                            file.dataFileId, file.tableId, "s3://hoglake-it/${f.cat}/raw", "parquet", 7, 101, 11,
                            3, 0, null, null, "provided", null, null, to,
                        ),
                    ),
                )
            assertThat(row).isEqualTo(expected)
            assertThat(read.request["sql"].asText()).isEqualTo("SELECT $projected$suffix")
            assertThat(
                read.request["args"],
            ).isEqualTo(json.readTree(json.writeValueAsBytes(listOf(file.tableId, from, to))))
            assertThat(json.readTree(read.bytes)["cols"][12].asText()).isEqualTo("stats_json")
            assertThat(json.readTree(read.bytes)["cols"][13].asText()).isEqualTo("split_offsets_json")
            assertThat(HogliteSql.FILE_COLUMNS).isEqualTo(full)
            capture.reference.set(true)
            capture.reads.clear()
            val referencePlan = hg.changes(f.cat, "ns", "t", from, to)
            val referenceTrace = capture.reads.toList()
            assertThat(referencePlan).isEqualTo(plan)
            assertThat(
                referenceTrace.map { it.path to it.request },
            ).isEqualTo(projectedTrace.map { it.path to it.request })
            val reference = referenceTrace.single { "f.begin_snapshot > ?" in it.request["sql"].asText() }
            val referenceRow = json.readTree(reference.bytes)["rows"].single()
            assertThat(json.readTree(referenceRow[12].asText())).hasSize(1)
            assertThat(json.readTree(referenceRow[13].asText())).isEqualTo(json.readTree("[4,40]"))
            assertThat(reference.bytes.size).isGreaterThan(read.bytes.size)
            println(
                "projection protocol bytes: projected=${read.bytes.size}, full=${reference.bytes.size}; " +
                    "one API-written file",
            )
            println(
                "PROJECTION_EVIDENCE " +
                    json.writeValueAsString(
                        mapOf(
                            "tenant_from_binding" to tenant,
                            "projected_request" to read.request,
                            "projected_body_utf8" to String(read.bytes, Charsets.UTF_8),
                            "full_reference_body_utf8" to String(reference.bytes, Charsets.UTF_8),
                            "projected_bytes" to read.bytes.size,
                            "full_reference_bytes" to reference.bytes.size,
                            "trace" to projectedTrace.map { mapOf("path" to it.path, "request" to it.request) },
                            "public_models_equal" to (referencePlan == plan),
                            "fixture" to "one API-written stats-bearing file; no physical I/O claim",
                        ),
                    ),
            )
            val dvSql =
                "SELECT delete_file_id, data_file_id, path, delete_count, file_size_bytes, begin_snapshot " +
                    "FROM dvs WHERE table_id = ? AND begin_snapshot > ? AND begin_snapshot <= ? " +
                    "ORDER BY begin_snapshot, delete_file_id"
            assertThat(projectedTrace.count { it.request["sql"].asText() == dvSql }).isEqualTo(1)
        }

    @Test
    fun `hoglite listing and scan retain their complete stored metadata`() =
        fixture(Backend.HOGLITE) { f ->
            f.append("metadata", stats = true)
            f.alter(listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)))))
            val listed = f.store.listFiles(f.cat, "ns", "t").single()
            assertThat(listed.orderingBounds).isInstanceOf(FileOrderingBounds.SortKey::class.java)
            val scanned =
                f.store.planScan(
                    f.cat,
                    "ns",
                    "t",
                    columnStats = ScanService.ColumnStatsRequest(null),
                    splitOffsets = true,
                ).single().dataFile
            assertThat(scanned.columnStats).hasSize(1)
            assertThat(scanned.splitOffsets).containsExactly(4L, 40L)
            assertThat(scanned.orderingBounds).isNull()
        }
}
