package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileOrderingBounds
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.SplitOffsets
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import com.sun.net.httpserver.HttpServer
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
        body: (CatalogStore, String) -> Unit,
    ) {
        afterSelection = null
        queries.clear()
        val lite = backend == Backend.HOGLITE
        var proxy: HttpServer? = null
        var client: HogliteClient? = null
        val executor = Executors.newFixedThreadPool(4)
        val store: CatalogStore
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
            val hg = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
            store = hg
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
        }
        try {
            run {
                val cat = "stats" + UUID.randomUUID().toString().replace("-", "")
                store.createCatalog(cat, "s3://hoglake-it/$cat")
                store.createNamespace(cat, "ns")
                store.createTable(cat, "ns", "t", columns)
                body(store, cat)
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

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `no provided files skip metadata and captured pending never acquires hydrated stats`(backend: Backend) =
        fixture(backend, listOf(ColumnDef("id", ColType.LONG))) { store, cat ->
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
        fixture(Backend.HOGLITE, listOf(ColumnDef("id", ColType.LONG))) { store, cat ->
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
            assertThat(queries.none { "SELECT tenant FROM catalogs WHERE name" in it }).isTrue()
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `statistics resolve against selected read metadata not later DDL`(backend: Backend) =
        fixture(backend, listOf(ColumnDef("id", ColType.LONG), ColumnDef("extra", ColType.LONG))) { store, cat ->
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

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `a wide table's stats are served whole and narrowed by stats_fields before any budget`(backend: Backend) =
        fixture(backend, (1..1000).map { ColumnDef("c$it", ColType.LONG) }) { store, cat ->
            val stats = (1..1000L).map { ColumnStats(it, 1, 0, null, null, null, null) }
            // The precondition: one file's stored stats exceed a scan page's per-row share.
            assertThat(json.writeValueAsBytes(stats).size).isGreaterThan(HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES)
            store.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf("a", "b", "c").map {
                                    FileRegistration("s3://hoglake-it/$cat/$it", 1, 100, 50, columnStats = stats)
                                },
                            ),
                        ),
                ),
            )
            queries.clear()
            val one = store.planScan(cat, "ns", "t", columnStats = ScanService.ColumnStatsRequest(setOf(7)))
            assertThat(one.map { f -> f.dataFile.columnStats!!.map { it.fieldId } }).allSatisfy {
                assertThat(it).containsExactly(7L)
            }
            if (backend == Backend.HOGLITE) {
                // Narrowed in the engine: one field fits the page, nothing is read alone.
                assertThat(queries.none { READ_ALONE in it }).isTrue()
            }
            queries.clear()
            val all = store.planScan(cat, "ns", "t", columnStats = ScanService.ColumnStatsRequest())
            assertThat(all.map { f -> f.dataFile.columnStats!!.map { it.fieldId } }).allSatisfy {
                assertThat(it).isEqualTo((1..1000L).toList())
            }
            if (backend == Backend.HOGLITE) {
                // The page's three oversized payloads share one query.
                assertThat(queries.count { READ_ALONE in it }).isEqualTo(1)
            }
            // Narrowed but still past the per-row share: read alone, and narrowed there too.
            val many = (1..900L).toSet()
            assertThat(json.writeValueAsBytes(stats.filter { it.fieldId in many }).size)
                .isGreaterThan(HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES)
            queries.clear()
            val most = store.planScan(cat, "ns", "t", columnStats = ScanService.ColumnStatsRequest(many))
            assertThat(most.map { f -> f.dataFile.columnStats!!.map { it.fieldId } }).allSatisfy {
                assertThat(it).isEqualTo(many.toList())
            }
            if (backend == Backend.HOGLITE) {
                val alone = queries.filter { READ_ALONE in it }
                assertThat(alone).hasSize(1)
                assertThat(alone.single()).contains("WITH wanted")
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `a file committed with the most split offsets is served by every split_offsets scan`(backend: Backend) =
        fixture(backend, listOf(ColumnDef("id", ColType.LONG))) { store, cat ->
            val offsets = (0 until SplitOffsets.MAX_ROW_GROUPS).map { 4L + it * 1_000L }
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
                                        200_000_000,
                                        50,
                                        splitOffsets = offsets,
                                    ),
                                    FileRegistration("s3://hoglake-it/$cat/b", 1, 100, 50, splitOffsets = listOf(4L)),
                                ),
                            ),
                        ),
                ),
            )
            val plan = store.planScan(cat, "ns", "t", splitOffsets = true)
            assertThat(plan.map { it.dataFile.splitOffsets }).containsExactly(offsets, listOf(4L))
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `a sorted file listing narrows a page whose leading sort bounds exceed the engine cap`(backend: Backend) =
        fixture(backend, listOf(ColumnDef("s", ColType.STRING))) { store, cat ->
            val alters = if (backend == Backend.HOGLITE) live!!.store.alters else AlterService(pg!!.jdbi)
            alters.alterTable(
                cat,
                "ns",
                "t",
                listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)))),
            )
            val bound = ByteArray(100 * 1024) { 'a'.code.toByte() }
            val stats = listOf(ColumnStats(1, 10, 0, null, null, bound, bound))
            // The precondition: a full page of the leading field's entry alone is past hoglited's 64 MiB cap.
            assertThat(json.writeValueAsBytes(stats).size.toLong() * HogliteCatalogStore.SCAN_PAGE_SIZE)
                .isGreaterThan(64L * 1024 * 1024)
            store.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$cat/a", 10, 100, 50, columnStats = stats)),
                            ),
                        ),
                ),
            )
            // Copied in place: one commit of every file would be past hoglited's request cap.
            val count = HogliteCatalogStore.SCAN_PAGE_SIZE + 1
            write(
                backend,
                cat,
                """WITH RECURSIVE n(i) AS (VALUES(2) UNION ALL SELECT i + 1 FROM n WHERE i < $count)
                    INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start, spec_id,
                                      partition_values_json, stats_state, stats_json, split_offsets_json, begin_snapshot)
                    SELECT i, table_id, path || i, record_count, file_size_bytes, i, spec_id,
                           partition_values_json, stats_state, stats_json, split_offsets_json, begin_snapshot
                      FROM n CROSS JOIN files
                     WHERE files.id = 1""",
                """INSERT INTO hog_data_file(catalog_id, data_file_id, table_id, begin_snapshot, path, record_count,
                                            file_size_bytes, row_id_start, stats_state, spec_id)
                    SELECT catalog_id, i, table_id, begin_snapshot, path || i, record_count,
                           file_size_bytes, i, stats_state, spec_id
                      FROM generate_series(2, $count) i CROSS JOIN hog_data_file
                     WHERE catalog_id = :cat AND data_file_id = 1;
                   INSERT INTO hog_file_column_stats
                    SELECT catalog_id, i, field_id, value_count, null_count, nan_count, size_bytes,
                           lower_bound, upper_bound
                      FROM generate_series(2, $count) i CROSS JOIN hog_file_column_stats
                     WHERE catalog_id = :cat AND data_file_id = 1""",
            )
            val files = store.listFiles(cat, "ns", "t", null, null, null, false, null, 0, emptyMap())
            assertThat(files.map { it.dataFileId }).isEqualTo((1L..count).toList())
            assertThat(files.map { it.orderingBounds }).allSatisfy { bounds ->
                val column = (bounds as FileOrderingBounds.SortKey).column
                assertThat(column.fieldId).isEqualTo(1L)
                assertThat(column.stats.lowerBound).isEqualTo(bound)
                assertThat(column.stats.upperBound).isEqualTo(bound)
            }
        }

    @Test
    fun `hoglite charges a read-alone payload that widened after its page measured it`() =
        fixture(Backend.HOGLITE, listOf(ColumnDef("id", ColType.LONG))) { store, cat ->
            store.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf("a", "b", "c", "d").map {
                                    FileRegistration("s3://hoglake-it/$cat/$it", 1, 100, 50)
                                },
                            ),
                        ),
                ),
            )
            // Four 15 MiB payloads, each read alone: 60 MiB of the 64 MiB metadata cap.
            write(Backend.HOGLITE, cat, "UPDATE files SET stats_json = printf('%*s[]', ${15 shl 20}, '')", "")
            assertThat(store.planScan(cat, "ns", "t", columnStats = ScanService.ColumnStatsRequest())).hasSize(4)
            // Rewritten in place between the page and its read-alone query, as a
            // concurrent type promotion does: 5 MiB more than the page charged.
            afterSelection = {
                write(
                    Backend.HOGLITE,
                    cat,
                    "UPDATE files SET stats_json = printf('%*s[]', ${20 shl 20}, '') WHERE id = 4",
                    "",
                )
            }
            assertThatThrownBy { store.planScan(cat, "ns", "t", columnStats = ScanService.ColumnStatsRequest()) }
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("scan plan requested metadata exceeds maximum")
            assertThat(afterSelection == null).describedAs("selection hook actually fired").isTrue()
        }

    private companion object {
        /** The read-alone query's filter: present only when a payload is read outside its page. */
        const val READ_ALONE = "WHERE f.id IN (SELECT value FROM json_each(?))"
    }
}
