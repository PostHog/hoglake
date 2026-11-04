package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
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
import java.util.UUID
import java.util.concurrent.Executors

/** The PG arm must run without an engine. Local bulk rows are fixture setup, not capacity claims. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteScanStatsParityIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private val json = jacksonObjectMapper()
    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null
    private var afterSelection: (() -> Unit)? = null
    private val queries = mutableListOf<String>()

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    private fun fixture(
        backend: Backend,
        columns: List<ColumnDef>,
        body: (CatalogStore, App, String) -> Unit,
    ) {
        afterSelection = null
        queries.clear()
        val lite = backend == Backend.HOGLITE
        var proxy: HttpServer? = null
        var client: HogliteClient? = null
        val executor = Executors.newFixedThreadPool(4)
        val store: CatalogStore
        val app: App
        if (lite) {
            val engine = live ?: HogliteConformanceEngine().also { live = it }
            val http = HttpClient.newHttpClient()
            proxy =
                HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                    this.executor = executor
                    createContext("/") { ex ->
                        val raw = ex.requestBody.readBytes()
                        val sql = json.readTree(raw)?.get("sql")?.asText().orEmpty()
                        synchronized(queries) { queries += sql }
                        val answer =
                            http.send(
                                HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                                    .header("Content-Type", "application/json")
                                    .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(raw)).build(),
                                HttpResponse.BodyHandlers.ofByteArray(),
                            )
                        if ("LEFT JOIN dvs d" in sql) {
                            val action = afterSelection
                            afterSelection = null
                            action?.invoke()
                        }
                        ex.sendResponseHeaders(answer.statusCode(), answer.body().size.toLong())
                        ex.responseBody.use { it.write(answer.body()) }
                    }
                    start()
                }
            client = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}", sleeper = {})
            val hg = HogliteCatalogStore(client)
            store = hg
            app =
                App.buildHoglite(
                    engine.config().copy(database = DatabaseBackend.HOGLITE, hogliteUrl = engine.url()),
                    hg, HealthProbe.forProbe(1000) { true },
                )
        } else {
            val db = pg ?: PgTestSupport.freshDatabase().also { pg = it }
            db.jdbi.setSqlLogger(
                object : SqlLogger {
                    override fun logAfterExecution(context: StatementContext) {
                        val sql = context.renderedSql
                        queries += sql
                        if ("LEFT JOIN hog_delete_file" in sql) {
                            val action = afterSelection
                            afterSelection = null
                            action?.invoke()
                        }
                    }
                },
            )
            store = PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi))
            app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
        }
        try {
            app.use {
                val cat = "stats" + UUID.randomUUID().toString().replace("-", "")
                store.createCatalog(cat, "s3://hoglake-it/$cat")
                store.createNamespace(cat, "ns")
                store.createTable(cat, "ns", "t", columns)
                body(store, app, cat)
            }
        } finally {
            pg?.jdbi?.setSqlLogger(SqlLogger.NOP_SQL_LOGGER)
            client?.close()
            proxy?.stop(0)
            executor.shutdownNow()
        }
    }

    private fun write(
        backend: Backend,
        cat: String,
        lite: String,
        postgres: String,
    ) {
        if (backend == Backend.HOGLITE) {
            live!!.store.catalogOperation(cat) {
                live!!.store.runTx(cat, HogliteCatalogStore.freshTxid("fixture"), null, HogliteTx().apply { add(lite) })
            }
        } else {
            pg!!.jdbi.useHandle<Exception> {
                it.execute(postgres.replace(":cat", "(SELECT catalog_id FROM hog_catalog WHERE name = '$cat')"))
            }
        }
    }

    private fun seed(
        backend: Backend,
        cat: String,
        provided: Int,
        pending: Int,
        failed: Int,
    ) {
        val total = provided + pending + failed
        write(
            backend,
            cat,
            """INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start,stats_state,begin_snapshot)
               WITH RECURSIVE g(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM g WHERE i < $total)
               SELECT i,1,'s3://hoglake-it/$cat/'||i,1,100,i,
                 CASE WHEN i <= $provided THEN 'provided' WHEN i <= ${provided + pending} THEN 'pending'
                 ELSE 'failed' END,1 FROM g""",
            """INSERT INTO hog_data_file(catalog_id,data_file_id,table_id,path,record_count,file_size_bytes,
                                        row_id_start,stats_state,begin_snapshot)
               SELECT :cat,i,(SELECT table_id FROM hog_table WHERE catalog_id=:cat),
                 's3://hoglake-it/$cat/'||i,1,100,i,
                 CASE WHEN i <= $provided THEN 'provided' WHEN i <= ${provided + pending} THEN 'pending'
                 ELSE 'failed' END,1 FROM generate_series(1,$total) i""",
        )
    }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `route cap charges provided files and conservatively counts sparse visible leaves`(backend: Backend) =
        fixture(backend, (1..100).map { ColumnDef("c$it", ColType.LONG) }) { _, app, cat ->
            seed(backend, cat, 10_000, 1, 1)
            testApplication {
                application { app.module(this) }
                val url = "/v1/catalogs/$cat/namespaces/ns/tables/t/scan"
                val exact = client.get("$url?include=column_stats")
                assertThat(exact.status).isEqualTo(HttpStatusCode.OK)
                val files = json.readTree(exact.bodyAsText())
                assertThat(files).hasSize(10_002)
                assertThat(files.count { it["data_file"].has("column_stats") }).isEqualTo(10_000)
                assertThat(files.first()["data_file"]["column_stats"]).isEmpty()
                write(
                    backend,
                    cat,
                    "UPDATE files SET stats_state='provided' WHERE id=10001",
                    "UPDATE hog_data_file SET stats_state='provided' WHERE catalog_id=:cat AND data_file_id=10001",
                )
                val over = client.get("$url?include=column_stats")
                assertThat(over.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(over.bodyAsText()).contains("1000100", "1000000", "stats_fields")
                assertThat(client.get("$url?include=column_stats&stats_fields=1,999999").status)
                    .isEqualTo(HttpStatusCode.OK)
                assertThat(client.get(url).status).isEqualTo(HttpStatusCode.OK)
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `route rejects exactly one million and one possible sparse entries`(backend: Backend) =
        fixture(backend, (1..101).map { ColumnDef("c$it", ColType.LONG) }) { _, app, cat ->
            seed(backend, cat, 9_901, 0, 0)
            testApplication {
                application { app.module(this) }
                val url = "/v1/catalogs/$cat/namespaces/ns/tables/t/scan?include=column_stats"
                val response = client.get(url)
                assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(response.bodyAsText()).contains("1000001")
                assertThat(client.get("$url&stats_fields=1,999999").status).isEqualTo(HttpStatusCode.OK)
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `PG leaf contract counts VARIANT but not populated containers`(backend: Backend) =
        fixture(
            backend,
            (1..98).map { ColumnDef("c$it", ColType.LONG) } +
                listOf(
                    ColumnDef("v", ColType.VARIANT),
                    ColumnDef("nested", ColType.STRUCT, children = listOf(ColumnDef("child", ColType.LONG))),
                ),
        ) { store, app, cat ->
            seed(backend, cat, 10_001, 0, 0)
            val forest = store.getTable(cat, "ns", "t").columns
            val ids = forest.flatMap { it.selfAndDescendants() }.associate { it.def.name to it.fieldId }
            val childId = ids.getValue("child")
            val encoded = json.writeValueAsString(listOf(ColumnStats(childId, 1, 0, null, null, null, null)))
            write(
                backend,
                cat,
                "UPDATE files SET stats_json='$encoded' WHERE id=1",
                "INSERT INTO hog_file_column_stats(catalog_id,data_file_id,field_id,value_count,null_count) " +
                    "VALUES(:cat,1,$childId,1,0)",
            )
            testApplication {
                application { app.module(this) }
                val url = "/v1/catalogs/$cat/namespaces/ns/tables/t/scan?include=column_stats"
                assertThat(client.get(url).status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                write(
                    backend,
                    cat,
                    "UPDATE files SET stats_state='pending' WHERE id=10001",
                    "UPDATE hog_data_file SET stats_state='pending' WHERE catalog_id=:cat AND data_file_id=10001",
                )
                assertThat(client.get(url).status).describedAs("100 leaves, not 101 tree nodes, per provided file")
                    .isEqualTo(HttpStatusCode.OK)
                // Include a container, a VARIANT, and a nonexistent id: only VARIANT charges.
                val narrow = client.get("$url&stats_fields=${ids.getValue("nested")},${ids.getValue("v")},999999")
                assertThat(narrow.status).isEqualTo(HttpStatusCode.OK)
                assertThat(json.readTree(narrow.bodyAsText()).first()["data_file"]["column_stats"]).isEmpty()
                val leaf = client.get("$url&stats_fields=$childId,999999")
                assertThat(leaf.status).isEqualTo(HttpStatusCode.OK)
                val attached = json.readTree(leaf.bodyAsText()).first()["data_file"]["column_stats"].single()
                assertThat(attached["field_id"].asLong()).isEqualTo(childId)
                assertThat(attached.has("path")).describedAs("scan DTO remains slim").isFalse()
                val resolved =
                    store.planScan(cat, "ns", "t", columnStats = ScanService.ColumnStatsRequest(setOf(childId)))
                        .first().dataFile.columnStats!!.single()
                assertThat(resolved.path).isEqualTo("nested.child")
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `no provided files skip metadata and captured pending never acquires hydrated stats`(backend: Backend) =
        fixture(backend, listOf(ColumnDef("id", ColType.LONG))) { store, _, cat ->
            val stats = listOf(ColumnStats(1, 1, 0, null, null, null, null))
            store.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(
                                    FileRegistration("s3://hoglake-it/$cat/pending", 1, 100, 50),
                                    FileRegistration("s3://hoglake-it/$cat/failed", 1, 100, 50),
                                    FileRegistration("s3://hoglake-it/$cat/provided", 1, 100, 50),
                                ),
                            ),
                        ),
                ),
            )
            val encoded = json.writeValueAsString(stats).replace("'", "''")
            // Half-written stats exist already. Only the captured file state authorizes attachment.
            write(
                backend,
                cat,
                "UPDATE files SET stats_json='$encoded', stats_state=CASE WHEN id=1 THEN 'pending' ELSE 'failed' END",
                "INSERT INTO hog_file_column_stats(catalog_id,data_file_id,field_id,value_count,null_count) " +
                    "SELECT :cat,data_file_id,1,1,0 FROM hog_data_file WHERE catalog_id=:cat",
            )
            write(
                backend,
                cat,
                "UPDATE files SET stats_state='failed' WHERE id=2",
                "UPDATE hog_data_file SET stats_state='failed' WHERE catalog_id=:cat AND data_file_id=2",
            )
            queries.clear()
            val request = ScanService.ColumnStatsRequest()
            val before = store.planScan(cat, "ns", "t", columnStats = request)
            assertThat(before.map { it.dataFile.columnStats }).containsOnlyNulls()
            assertThat(queries.none { "FROM columns " in it || "FROM hog_column" in it }).isTrue()
            write(
                backend,
                cat,
                "UPDATE files SET stats_state='provided' WHERE id=3",
                "UPDATE hog_data_file SET stats_state='provided' WHERE catalog_id=:cat AND data_file_id=3",
            )
            afterSelection = {
                write(
                    backend, cat, "UPDATE files SET stats_state='provided' WHERE id=1",
                    "UPDATE hog_data_file SET stats_state='provided' WHERE catalog_id=:cat AND data_file_id=1",
                )
            }
            val captured = store.planScan(cat, "ns", "t", columnStats = request)
            assertThat(afterSelection == null).describedAs("selection hook actually fired").isTrue()
            assertThat(captured.map { it.dataFile.statsState })
                .containsExactly(StatsState.PENDING, StatsState.FAILED, StatsState.PROVIDED)
            assertThat(captured.take(2).map { it.dataFile.columnStats }).containsOnlyNulls()
            assertThat(captured[2].dataFile.columnStats!!.map { it.fieldId }).containsExactly(1L)
            val next = store.planScan(cat, "ns", "t", columnStats = request)
            assertThat(next[0].dataFile.columnStats!!.map { it.fieldId }).containsExactly(1L)
            assertThat(next[1].dataFile.columnStats).isNull()
        }

    @Test
    fun `hoglite captured scan refuses an overtaking floor without rebinding`() =
        fixture(Backend.HOGLITE, listOf(ColumnDef("id", ColType.LONG))) { store, _, cat ->
            store.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "t", listOf(FileRegistration("s3://hoglake-it/$cat/a", 1, 100, 50))),
                        ),
                ),
            )
            val at = store.getCatalog(cat).headSnapshotId
            queries.clear()
            afterSelection = {
                live!!.store.commit(
                    cat,
                    CommitRequest(
                        appends =
                            listOf(
                                TableAppend("ns", "t", listOf(FileRegistration("s3://hoglake-it/$cat/b", 1, 100, 50))),
                            ),
                    ),
                )
                write(
                    Backend.HOGLITE, cat, "UPDATE meta SET earliest_snapshot_id=${at + 1}",
                    "",
                )
            }
            assertThatThrownBy {
                store.planScan(
                    cat,
                    "ns",
                    "t",
                    snapshot = at,
                    columnStats = ScanService.ColumnStatsRequest(),
                )
            }
                .isInstanceOf(HoglakeException.Expired::class.java)
            assertThat(afterSelection == null).isTrue()
            assertThat(queries.count { "SELECT tenant FROM catalogs WHERE name" in it }).isEqualTo(1)
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `statistics resolve against selected read metadata not later DDL`(backend: Backend) =
        fixture(backend, listOf(ColumnDef("id", ColType.LONG), ColumnDef("extra", ColType.LONG))) { store, _, cat ->
            store.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(
                                    FileRegistration(
                                        "s3://hoglake-it/$cat/a",
                                        1,
                                        100,
                                        50,
                                        columnStats = listOf(ColumnStats(2, 1, 0, null, null, null, null)),
                                    ),
                                ),
                            ),
                        ),
                ),
            )
            afterSelection = {
                val alters = if (backend == Backend.HOGLITE) live!!.store.alters else AlterService(pg!!.jdbi)
                alters.alterTable(cat, "ns", "t", listOf(AlterOp.RenameColumn("extra", "renamed")))
            }
            val plan = store.planScan(cat, "ns", "t", columnStats = ScanService.ColumnStatsRequest(setOf(2, 999999)))
            assertThat(afterSelection == null).isTrue()
            val stat = plan.single().dataFile.columnStats!!.single()
            assertThat(stat.name).isEqualTo("extra")
            assertThat(stat.path).isEqualTo("extra")
            assertThat(stat.fieldId).isEqualTo(2L)
            assertThat(store.getTable(cat, "ns", "t").columns.last().def.name).isEqualTo("renamed")
        }
}
