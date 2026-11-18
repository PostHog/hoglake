package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.TableCreationDefinition
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/** Direct store scenarios use the published engine and a test tenant resolver. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteBackendIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private val client get() = engine.client
    private val store get() = engine.store
    private val metricsUrl get() = engine.url() + "/metrics"

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun close() {
        if (::engine.isInitialized) engine.close()
    }

    private fun freshCatalog(): String {
        val name = HogliteConformanceEngine.uniqueName("backend")
        store.createCatalog(name, "s3://hoglake-it/$name")
        return name
    }

    private fun scaleWrite(
        catalog: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(catalog) {
        store.runTx(catalog, HogliteCatalogStore.freshTxid("scale"), null, HogliteTx().apply { add(sql, *args) })
    }

    private fun queryCount(): Long {
        val body =
            HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(metricsUrl)).build(),
                HttpResponse.BodyHandlers.ofString(),
            ).body()
        return body.lineSequence().first { it.startsWith("hoglite_queries_total ") }.substringAfter(' ').toLong()
    }

    private fun q(
        catalog: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(catalog) { store.queryCatalog(catalog, sql, *args) }

    private fun append(
        catalog: String,
        path: String,
        rows: Long = 10,
        key: UUID? = UUID.randomUUID(),
        read: Long? = null,
        table: String = "t",
    ) = store.commit(
        catalog,
        CommitRequest(
            readSnapshot = read,
            idempotencyKey = key,
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        table,
                        listOf(FileRegistration("s3://hoglake-it/$catalog/data/$path", rows, 100, 50)),
                    ),
                ),
        ),
    )

    private fun dv(
        catalog: String,
        fileId: Long,
        read: Long,
        path: String,
        count: Long,
        table: String = "t",
    ) = store.commit(
        catalog,
        CommitRequest(
            readSnapshot = read,
            idempotencyKey = UUID.randomUUID(),
            deletes =
                listOf(
                    TableDeletes(
                        "ns",
                        table,
                        listOf(
                            DeleteFileRegistration(fileId, "s3://hoglake-it/$catalog/data/$path", count, 10),
                        ),
                    ),
                ),
        ),
    )

    @Test
    fun `scan pages 565000 files by row lineage and id without fetching unrequested stats`() {
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val count = 565_000
        for (start in 1..count step 5_000) {
            scaleWrite(
                cat,
                """WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i + 1 FROM n WHERE i < ?)
                    INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start, stats_state, begin_snapshot)
                    SELECT i, ?, 's3://hoglake-it/scale/' || i || '.parquet', 1, 100, i / 2, 'pending', ? FROM n""",
                start,
                minOf(start + 4_999, count),
                table.tableId,
                table.readSnapshotId,
            )
        }
        // Even a legacy oversized stats payload must cost no wire bytes when
        // stats were not requested. Row-id ties cross page boundaries.
        scaleWrite(
            cat,
            "UPDATE files SET stats_json = ? WHERE id = 1",
            "x".repeat(HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES + 1),
        )
        val before = queryCount()
        val started = System.nanoTime()
        val files = store.planScan(cat, "ns", "t", table.readSnapshotId, null, null, false)
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        val queries = queryCount() - before
        assertThat(files).hasSize(count)
        assertThat(
            files.withIndex().all {
                    (i, f) ->
                f.dataFile.dataFileId == i + 1L && f.dataFile.rowIdStart == (i + 1L) / 2
            },
        ).isTrue()
        assertThat(files.all { it.dataFile.columnStats == null && it.dataFile.splitOffsets == null }).isTrue()
        val pages = (count + HogliteCatalogStore.SCAN_PAGE_SIZE - 1) / HogliteCatalogStore.SCAN_PAGE_SIZE
        assertThat(queries).isBetween(pages.toLong(), pages + 30L)
        println("hoglite scale: $count files, $pages pages, $queries queries, $elapsedMs ms")
    }

    @Test
    fun `oversized requested scan metadata fails explicitly instead of timing out`() {
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        append(cat, "a.parquet")
        scaleWrite(
            cat,
            "UPDATE files SET stats_json = ?, split_offsets_json = ?",
            "x".repeat(HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES + 1),
            "x".repeat(HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES + 1),
        )
        assertThatThrownBy {
            store.planScan(
                cat,
                "ns",
                "t",
                null,
                null,
                com.posthog.hoglake.service.ScanService.ColumnStatsRequest(),
                false,
            )
        }.isInstanceOf(
            HoglakeException.Validation::class.java,
        ).hasMessageContaining("scan plan file metadata exceeds maximum")
        assertThatThrownBy {
            store.planScan(cat, "ns", "t", null, null, null, true)
        }.isInstanceOf(
            HoglakeException.Validation::class.java,
        ).hasMessageContaining("scan plan file metadata exceeds maximum")
        // Individually legal payloads must not accumulate without a bound.
        val payload = " ".repeat(HogliteCatalogStore.SCAN_MAX_ROW_JSON_BYTES - 2) + "[]"
        scaleWrite(cat, "UPDATE files SET stats_json = ?, split_offsets_json = NULL", payload)
        scaleWrite(
            cat,
            """WITH RECURSIVE n(i) AS (VALUES(2) UNION ALL SELECT i + 1 FROM n WHERE i < 1025)
                INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start,
                                  stats_state, stats_json, begin_snapshot)
                SELECT i, table_id, path || i, record_count, file_size_bytes, i,
                       stats_state, stats_json, begin_snapshot FROM n CROSS JOIN files WHERE files.id = 1""",
        )
        assertThatThrownBy {
            store.planScan(
                cat,
                "ns",
                "t",
                null,
                null,
                com.posthog.hoglake.service.ScanService.ColumnStatsRequest(),
                false,
            )
        }.isInstanceOf(
            HoglakeException.Validation::class.java,
        ).hasMessageContaining("scan plan requested metadata exceeds maximum")
    }

    @Test
    fun `scan retries a real checkpoint-cancelled page without losing its tenant binding`() {
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        append(cat, "a.parquet")
        val engine = metricsUrl.removeSuffix("/metrics")
        val http = HttpClient.newHttpClient()
        val json = jacksonObjectMapper()
        val attempts = AtomicInteger()
        val cancelledStatus = AtomicInteger()
        val cancelledCode = AtomicReference<String>()
        val cancelledError = AtomicReference<String>()
        val scanTenant = AtomicReference<String>()
        val failure = AtomicReference<Throwable>()
        val executor = Executors.newCachedThreadPool()
        val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        proxy.executor = executor
        proxy.createContext("/") { ex ->
            try {
                val path = ex.requestURI.toString()
                var body = ex.requestBody.readBytes()
                val scanPage =
                    path.endsWith("/query") &&
                        body.decodeToString().contains("ORDER BY f.row_id_start, f.id LIMIT")
                val first = scanPage && attempts.incrementAndGet() == 1
                if (scanPage) {
                    if (first) scanTenant.set(path) else check(path == scanTenant.get())
                }
                if (first) {
                    val statement = json.readTree(body) as ObjectNode
                    statement.put(
                        "sql",
                        statement["sql"].asText().replace(
                            "FROM files f",
                            "FROM files f CROSS JOIN (WITH RECURSIVE n(x) AS " +
                                "(VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n) slow",
                        ),
                    )
                    body = json.writeValueAsBytes(statement)
                }
                val pending =
                    http.sendAsync(
                        HttpRequest.newBuilder(URI.create(engine + path)).header("Content-Type", "application/json")
                            .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                if (first) {
                    val tenantPath = path.removeSuffix("/query")
                    val deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos()
                    var active = false
                    while (System.nanoTime() < deadline && !active) {
                        val status =
                            http.send(
                                HttpRequest.newBuilder(URI.create("$engine$tenantPath/status")).build(),
                                HttpResponse.BodyHandlers.ofString(),
                            )
                        active = json.readTree(status.body())["local"]?.get("active_reads")?.asInt() == 1
                    }
                    check(active) { "slow scan did not enter a read snapshot" }
                    val release =
                        http.send(
                            HttpRequest.newBuilder(URI.create("$engine$tenantPath/release"))
                                .POST(HttpRequest.BodyPublishers.noBody()).build(),
                            HttpResponse.BodyHandlers.discarding(),
                        )
                    check(release.statusCode() == 200) { "checkpoint drain failed" }
                }
                val response = pending.get(5, TimeUnit.SECONDS)
                if (first) {
                    cancelledStatus.set(response.statusCode())
                    cancelledCode.set(json.readTree(response.body())["code"]?.asText())
                    cancelledError.set(json.readTree(response.body())["error"]?.asText())
                }
                ex.responseHeaders.add("Content-Type", "application/json")
                ex.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                ex.responseBody.use { it.write(response.body()) }
            } catch (e: Throwable) {
                failure.set(e)
                ex.sendResponseHeaders(599, -1)
                ex.close()
            }
        }
        proxy.start()
        try {
            HogliteClient("http://127.0.0.1:${proxy.address.port}", sleeper = {}).use { retrying ->
                val scoped = HogliteCatalogStore(retrying, HogliteConformanceEngine.TENANTS)
                val files = scoped.planScan(cat, "ns", "t", null, null, null, false)
                assertThat(files.map { it.dataFile.path }).containsExactly("s3://hoglake-it/$cat/data/a.parquet")
            }
            assertThat(failure.get()).isNull()
            assertThat(cancelledStatus.get()).isEqualTo(503)
            // Upstream engines before the structured taxonomy omit code.
            assertThat(cancelledCode.get()).isIn(null, "not_applied")
            assertThat(cancelledError.get()).isEqualTo("read canceled by checkpoint; retry")
            println(
                "hoglite checkpoint retry: status=${cancelledStatus.get()} " +
                    "code=${cancelledCode.get()} error=${cancelledError.get()} attempts=${attempts.get()}",
            )
            assertThat(attempts.get()).isEqualTo(2)
        } finally {
            proxy.stop(0)
            executor.shutdownNow()
        }
    }

    @Test
    fun `catalog namespace table lifecycle`() {
        val catalog = freshCatalog()
        // a same-name create is the 409, whatever the data_path.
        assertThatThrownBy { store.createCatalog(catalog, "s3://hoglake-it/$catalog") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        assertThatThrownBy { store.createCatalog(catalog, "s3://hoglake-it/other-$catalog") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)

        assertThat(store.getCatalog(catalog).headSnapshotId).isEqualTo(0)
        assertThat(store.listCatalogs(setOf(catalog)).map { it.name }).contains(catalog)

        val ns = store.createNamespace(catalog, "ns")
        assertThat(store.getNamespace(catalog, "ns").namespaceId).isEqualTo(ns.namespaceId)
        assertThatThrownBy { store.createNamespace(catalog, "ns") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)

        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThat(table.snapshotId).isNotNull()
        assertThat(store.getTable(catalog, "ns", "t").tableUuid).isEqualTo(table.tableUuid)
        assertThat(store.listTables(catalog, "ns").map { it.name }).containsExactly("t")

        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        assertThatThrownBy { store.getTable(catalog, "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        // time travel: the table is still visible at its live snapshot.
        assertThat(
            store.getTable(catalog, "ns", "t", snapshot = table.snapshotId).tableUuid,
        ).isEqualTo(table.tableUuid)

        store.dropNamespace(catalog, "ns", author = "test", message = "drop")
        assertThat(store.listNamespaces(catalog)).isEmpty()

        store.dropCatalog(catalog, "test", "drop")
        assertThatThrownBy { store.getCatalog(catalog) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    /**
     * A head read resolves the namespace in the same transaction as the
     * listing, whose namespace id is a subselect (#537). The subselect alone
     * makes a missing namespace an empty listing, so only the namespace
     * statement tells "missing" (NotFound) from "empty" (no rows).
     */
    @Test
    fun `head reads refuse a missing or dropped namespace and list an empty one`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "empty")
        store.createNamespace(catalog, "gone")
        store.dropNamespace(catalog, "gone", author = "test", message = "drop")
        val head = store.getCatalog(catalog).headSnapshotId

        assertThat(store.listTables(catalog, "empty")).isEmpty()
        for (ns in listOf("missing", "gone")) {
            assertThatThrownBy { store.listTables(catalog, ns) }
                .isInstanceOf(HoglakeException.NotFound::class.java)
                .hasMessage("namespace '$ns' in catalog '$catalog'")
            assertThatThrownBy { store.getTable(catalog, ns, "t") }
                .isInstanceOf(HoglakeException.NotFound::class.java)
                .hasMessage("namespace '$ns' in catalog '$catalog'")
        }
        assertThatThrownBy { store.getTable(catalog, "empty", "t", totals = true) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessage("table 'empty.t' in catalog '$catalog' at snapshot $head")
    }

    @Test
    fun `commit appends land with row lineage and replay idempotently`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val head = table.readSnapshotId

        val key = UUID.randomUUID()
        val request =
            CommitRequest(
                readSnapshot = head,
                idempotencyKey = key,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 100, 1000, 500),
                                FileRegistration("s3://hoglake-it/$catalog/data/b.parquet", 50, 800, 400),
                            ),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            )
        val committed = store.commit(catalog, request)
        assertThat(committed.snapshotId).isEqualTo(head + 1)
        // The same key + payload replays the receipt; it does not re-land.
        val replayed = store.commit(catalog, request)
        assertThat(replayed.snapshotId).isEqualTo(committed.snapshotId)
        assertThat(store.getCatalog(catalog).headSnapshotId).isEqualTo(head + 1)
        assertThat(store.receipt(catalog, key).snapshotId).isEqualTo(committed.snapshotId)
        // The same key with a DIFFERENT payload is the 422.
        assertThatThrownBy {
            store.commit(
                catalog,
                request.copy(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/c.parquet", 1, 1, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Row lineage is server-assigned and contiguous.
        val files = store.listFiles(catalog, "ns", "t")
        assertThat(files.map { it.recordCount }).containsExactly(100, 50)
        assertThat(files.map { it.rowIdStart }).containsExactly(0L, 100L)
        val scan = store.planScan(catalog, "ns", "t")
        assertThat(scan.map { it.dataFile.path }).containsExactly(
            "s3://hoglake-it/$catalog/data/a.parquet",
            "s3://hoglake-it/$catalog/data/b.parquet",
        )
        // The changefeed carries the insert's files in the window.
        val changes = store.changes(catalog, "ns", "t", fromSnapshot = head)
        assertThat(changes.files.map { it.path }).containsExactly(
            "s3://hoglake-it/$catalog/data/a.parquet",
            "s3://hoglake-it/$catalog/data/b.parquet",
        )
    }

    @Test
    fun `a prepared delete against a stale read snapshot is the typed conflict`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val head = table.readSnapshotId
        val first =
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = head,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 10, 100, 50)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        val fileId = store.listFiles(catalog, "ns", "t").single().dataFileId

        // A writer that read at `first.snapshotId` commits a data change;
        // a second writer's GUARDED delete against the same read snapshot
        // must conflict.
        store.commit(
            catalog,
            CommitRequest(
                readSnapshot = first.snapshotId,
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://hoglake-it/$catalog/data/b.parquet", 5, 50, 25)),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            ),
        )
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = first.snapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        fileId,
                                        "s3://hoglake-it/$catalog/data/a.puffin",
                                        1,
                                        10,
                                    ),
                                ),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)

        // A delete that re-reads lands, and the scan pairs the DV.
        val head2 = store.getCatalog(catalog).headSnapshotId
        val deleted =
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = head2,
                    idempotencyKey = UUID.randomUUID(),
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        fileId,
                                        "s3://hoglake-it/$catalog/data/a.puffin",
                                        1,
                                        10,
                                    ),
                                ),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        val scan = store.planScan(catalog, "ns", "t")
        val withDv = scan.filter { it.deleteFile != null }
        assertThat(withDv).hasSize(1)
        assertThat(withDv.single().deleteFile!!.deleteCount).isEqualTo(1)
        assertThat(deleted.snapshotId).isEqualTo(head2 + 1)
    }

    @Test
    fun `a guarded commit after drop plus recreate is the TableRecreated 409`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        val recreated = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = recreated.readSnapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 1, 1, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.TableRecreated::class.java)
    }

    @Test
    fun `offsets are monotonic and reject regression`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val offset =
            store.commitOffset(catalog, "consumer-1", table.tableUuid, table.readSnapshotId)
        assertThat(offset.committedSnapshot).isEqualTo(table.readSnapshotId)
        // An EQUAL re-commit is Postgres's upsert no-op arm: accepted, so a
        // client retry of a timed-out-but-applied call is safe.
        assertThat(store.commitOffset(catalog, "consumer-1", table.tableUuid, table.readSnapshotId).committedSnapshot)
            .isEqualTo(table.readSnapshotId)
        // Backwards is the 409.
        append(catalog, "offset.parquet")
        val head = store.getCatalog(catalog).headSnapshotId
        store.commitOffset(catalog, "consumer-1", table.tableUuid, head)
        assertThatThrownBy {
            store.commitOffset(catalog, "consumer-1", table.tableUuid, head - 1)
        }.isInstanceOf(HoglakeException.OffsetRegression::class.java)
        assertThat(store.getOffset(catalog, "consumer-1", table.tableUuid).committedSnapshot)
            .isEqualTo(head)
        assertThatThrownBy {
            store.commitOffset(catalog, "consumer-1", UUID.randomUUID(), 0)
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `an upload claim settles when its owner commits the path`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val owner = UUID.randomUUID()
        val claim = store.uploads.claim(catalog, UUID.randomUUID(), owner, "s3://hoglake-it/$catalog", "data")
        assertThat(claim.path).startsWith("s3://hoglake-it/$catalog/trino-upload/")
        // idempotent: the same upload id re-claims the same path.
        assertThat(store.uploads.claim(catalog, claim.uploadId, owner, "s3://hoglake-it/$catalog", "data").path)
            .isEqualTo(claim.path)
        // Valid footer metadata keeps both attempts focused on claim ownership.
        // a different owner committing the claimed path is fenced.
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = table.readSnapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration(claim.path, 1, 100, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        // the owner settles it.
        store.commit(
            catalog,
            CommitRequest(
                readSnapshot = table.readSnapshotId,
                idempotencyKey = owner,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration(claim.path, 1, 100, 1)),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            ),
        )
        // and a settled claim cannot be abandoned after the fact.
        assertThat(store.uploads.abandon(catalog, owner, listOf(claim.path))).isEqualTo(0)
    }

    @Test
    fun `prepare publish and the durable rejections`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val op = UUID.randomUUID()
        val definition =
            TableCreationDefinition(
                "ns",
                "created",
                listOf(ColumnDef("id", ColType.LONG)),
            )
        val prepared = store.tableCreations.prepare(catalog, op, definition)
        assertThat(prepared.state).isEqualTo("prepared")
        // idempotent replay
        assertThat(store.tableCreations.prepare(catalog, op, definition).tableUuid)
            .isEqualTo(prepared.tableUuid)
        // a different definition under the same id is the 409
        assertThatThrownBy {
            store.tableCreations.prepare(catalog, op, definition.copy(name = "other"))
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)

        val files =
            listOf(
                FileRegistration(
                    "${prepared.writePath}part-0.parquet",
                    10,
                    100,
                    50,
                ),
            )
        val published = store.tableCreations.publish(catalog, op, files)
        assertThat(published.state).isEqualTo("committed")
        assertThat(published.snapshotId).isNotNull()
        assertThat(store.getTable(catalog, "ns", "created").tableUuid).isEqualTo(prepared.tableUuid)
        assertThat(store.listFiles(catalog, "ns", "created").single().recordCount).isEqualTo(10)
        // publish replay: same files return the receipt; different files 409.
        assertThat(store.tableCreations.publish(catalog, op, files).state).isEqualTo("committed")
        assertThatThrownBy {
            store.tableCreations.publish(
                catalog,
                op,
                listOf(FileRegistration("${prepared.writePath}part-1.parquet", 1, 1, 1)),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)

        // target_exists: a prepared creation whose name was taken in the
        // meantime rejects DURABLY, and the receipt says why.
        val op2 = UUID.randomUUID()
        store.tableCreations.prepare(catalog, op2, definition.copy(name = "taken"))
        store.createTable(catalog, "ns", "taken", listOf(ColumnDef("id", ColType.LONG)))
        val rejected =
            store.tableCreations.publish(
                catalog,
                op2,
                emptyList(),
            )
        assertThat(rejected.state).isEqualTo("rejected")
        assertThat(rejected.reason).isEqualTo("target_exists")

        // abort fences a prepared receipt.
        val op3 = UUID.randomUUID()
        store.tableCreations.prepare(catalog, op3, definition.copy(name = "aborted"))
        assertThat(store.tableCreations.abort(catalog, op3).state).isEqualTo("aborted")
        assertThat(store.tableCreations.publish(catalog, op3, emptyList()).state).isEqualTo("aborted")
    }

    @Test
    fun `create-or-replace of a table absent at read_snapshot publishes a plain create`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val read = store.getCatalog(catalog).headSnapshotId
        val op = UUID.randomUUID()
        val definition =
            TableCreationDefinition(
                "ns",
                "fresh",
                listOf(ColumnDef("id", ColType.LONG)),
                replacement = com.posthog.hoglake.service.ReplacementTarget(null, read),
                author = "test",
                message = "create or replace",
            )
        val prepared = store.tableCreations.prepare(catalog, op, definition)
        assertThat(prepared.state).isEqualTo("prepared")
        val published =
            store.tableCreations.publish(
                catalog,
                op,
                listOf(FileRegistration("${prepared.writePath}part-0.parquet", 3, 100, 50)),
            )
        assertThat(published.state).isEqualTo("committed")
        val table = store.getTable(catalog, "ns", "fresh")
        assertThat(table.tableUuid).isEqualTo(prepared.tableUuid)
        assertThat(table.recordCount).isEqualTo(3L)
        // Nothing was retired: no drop change, no replaced id, and the new
        // incarnation's feed carries no replacement fence pair.
        val snap = published.snapshotId!!
        assertThat(
            q(catalog, "SELECT kind FROM changes WHERE snapshot_id = ? ORDER BY kind", snap).rows.map {
                it[0].asString()
            },
        ).containsExactly("table_created", "table_inserted_into")
        assertThat(
            q(catalog, "SELECT replaced_table_id FROM tables WHERE table_uuid = ?", prepared.tableUuid.toString())
                .firstRow()!![0],
        ).isNull()
        assertThat(published.reason).isNull()
    }

    @Test
    fun `rename and add column, guarded and unguarded`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val renamed =
            store.alters.alterTable(
                catalog,
                "ns",
                "t",
                listOf(com.posthog.hoglake.model.AlterOp.RenameTable("t2")),
                expectedTableUuid = table.tableUuid,
            )
        assertThat(renamed.name).isEqualTo("t2")
        assertThat(store.getTable(catalog, "ns", "t2").tableUuid).isEqualTo(table.tableUuid)
        assertThatThrownBy { store.getTable(catalog, "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        val added =
            store.alters.alterTable(
                catalog,
                "ns",
                "t2",
                listOf(
                    com.posthog.hoglake.model.AlterOp.AddColumn(ColumnDef("extra", ColType.STRING)),
                ),
            )
        assertThat(added.columns.map { it.def.name }).containsExactly("id", "extra")
        // a guarded alter against a stale read snapshot is the typed 409
        assertThatThrownBy {
            store.alters.alterTable(
                catalog,
                "ns",
                "t2",
                listOf(com.posthog.hoglake.model.AlterOp.RenameTable("t3")),
                readSnapshot = table.readSnapshotId,
            )
        }.isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
    }

    @Test
    fun `views create list drop with change rows`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        val view = store.views.create(catalog, "ns", "v", "SELECT 1", "trino", "test", "create")
        assertThat(store.views.get(catalog, "ns", "v").viewUuid).isEqualTo(view.viewUuid)
        assertThat(store.views.list(catalog, "ns").map { it.name }).containsExactly("v")
        assertThatThrownBy { store.views.create(catalog, "ns", "v", "SELECT 2", "trino", "test", "dup") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)

        store.views.drop(catalog, "ns", "v", "test", "drop")
        assertThat(store.views.list(catalog, "ns")).isEmpty()
        // The changefeed saw both ends of the lifecycle.
        val kinds = mutableListOf<String>()
        var from = 0L
        while (true) {
            val page = store.listSnapshots(catalog, from, 50)
            if (page.first.isEmpty()) break
            from = page.first.last().snapshotId
            if (!page.second) break
        }
        // (snapshots feed is the whole-catalog one; the view change kinds
        // ride it — this asserts the feed grew past the DDL, not the kind
        // strings, which the table-scoped feed owns.)
        assertThat(from).isGreaterThan(1L)
    }

    @Test
    fun `truncate ends files and dvs at the new snapshot`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val commit =
            store.commit(
                catalog,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                namespace = "ns",
                                table = "t",
                                files =
                                    listOf(
                                        FileRegistration(
                                            path = "s3://hoglake-it/$catalog/data/ns/t/a.parquet",
                                            recordCount = 10,
                                            fileSizeBytes = 100,
                                        ),
                                    ),
                            ),
                        ),
                ),
            )
        assertThat(store.listFiles(catalog, "ns", "t")).hasSize(1)

        val result =
            store.truncateTable(
                catalog,
                "ns",
                "t",
                expectedTableUuid = table.tableUuid,
                author = "test",
                message = "truncate",
            )
        assertThat(store.listFiles(catalog, "ns", "t")).isEmpty()
        // Time travel sees the file as of the commit.
        assertThat(
            store.listFiles(catalog, "ns", "t", snapshot = commit.snapshotId),
        ).hasSize(1)
        assertThat(result.snapshotId).isGreaterThan(commit.snapshotId)
    }

    @Test
    fun `partition stats rank by the planner's debt`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        repeat(3) { i ->
            store.commit(
                catalog,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                namespace = "ns",
                                table = "t",
                                files =
                                    listOf(
                                        FileRegistration(
                                            path = "s3://hoglake-it/$catalog/data/ns/t/f$i.parquet",
                                            recordCount = 10,
                                            fileSizeBytes = 100,
                                        ),
                                    ),
                            ),
                        ),
                ),
            )
        }
        val partitions =
            com.posthog.hoglake.hoglite.HoglitePartitions(
                store,
                smallFileThresholdBytes = 1_000_000,
                // Two, not the production default of five: three small
                // files must form a group for the debt assertion to bite.
                minInputFiles = 2,
            )
        val report = partitions.partitionStats(catalog, "ns", "t", 50)
        assertThat(report.partitions).hasSize(1)
        val debt = report.partitions.single()
        assertThat(debt.fileCount).isEqualTo(3)
        assertThat(debt.smallFileCount).isEqualTo(3)
        // Three files under the target pack into one group: all three are debt.
        assertThat(debt.debtScore).isEqualTo(3)
        val listing = partitions.listPartitions(catalog, "ns", "t")
        assertThat(listing.total).isEqualTo(1)
        assertThat(listing.partitions.single().recordCount).isEqualTo(30)
    }

    @Test
    fun `a superseded DV epoch is ended atomically, leaving exactly one live vector`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        append(catalog, "a.parquet")
        val f = store.listFiles(catalog, "ns", "t").single().dataFileId
        dv(catalog, f, store.getCatalog(catalog).headSnapshotId, "dv1.puffin", 1)
        dv(catalog, f, store.getCatalog(catalog).headSnapshotId, "dv2.puffin", 3)
        // The predecessor epoch was ended by the superseding commit: exactly
        // one live vector, and it is the newer one.
        val live =
            q(catalog, "SELECT path, delete_count FROM dvs WHERE data_file_id = ? AND end_snapshot IS NULL", f)
        assertThat(live.rows.size).isEqualTo(1)
        assertThat(live.rows.single()[0].asString()).endsWith("dv2.puffin")
        assertThat(live.rows.single()[1].asLong()).isEqualTo(3L)
        assertThat(
            q(catalog, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND end_snapshot IS NOT NULL", f)
                .firstRow()!![0].asLong(),
        ).isEqualTo(1L)
    }

    @Test
    fun `a DV against a retired file is the typed conflict, both directions`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val t = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        append(catalog, "a.parquet")
        val f = store.listFiles(catalog, "ns", "t").single().dataFileId
        val afterAppend = store.getCatalog(catalog).headSnapshotId
        store.truncateTable(catalog, "ns", "t", t.tableUuid, "test", "retire the file")
        // Retired AFTER the read snapshot: the retryable 409.
        assertThatThrownBy { dv(catalog, f, afterAppend, "dv.puffin", 1) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        // Retired BEFORE the read snapshot: the caller can see it, 422.
        val now = store.getCatalog(catalog).headSnapshotId
        assertThatThrownBy { dv(catalog, f, now, "dv.puffin", 1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `an unkeyed commit is never deduplicated, even byte-identical after truncate`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val t = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val first = append(catalog, "a.parquet", key = null)
        store.truncateTable(catalog, "ns", "t", t.tableUuid, "test", "clear the table")
        assertThat(store.listFiles(catalog, "ns", "t")).isEmpty()
        // The identical unkeyed commit must APPLY, not replay the old receipt.
        val second = append(catalog, "a.parquet", key = null)
        assertThat(second.snapshotId).isGreaterThan(first.snapshotId)
        assertThat(store.listFiles(catalog, "ns", "t")).hasSize(1)
    }

    @Test
    fun `a staged DV cannot reference another table's append by path`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.createTable(catalog, "ns", "u", listOf(ColumnDef("id", ColType.LONG)))
        val path = "s3://hoglake-it/$catalog/data/shared.parquet"
        // One commit: append the path to t, reference it from u's delete.
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    idempotencyKey = UUID.randomUUID(),
                    appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 10, 100, 50)))),
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "u",
                                listOf(
                                    DeleteFileRegistration(
                                        0,
                                        "s3://hoglake-it/$catalog/data/dv.puffin",
                                        1,
                                        10,
                                        dataFilePath = path,
                                    ),
                                ),
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        // Nothing landed.
        assertThat(store.listFiles(catalog, "ns", "t")).isEmpty()
        assertThat(store.listFiles(catalog, "ns", "u")).isEmpty()
    }

    @Test
    fun `a guarded alter on a fresh read snapshot applies, and one alter adds two columns`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val t = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val read = store.getTable(catalog, "ns", "t").readSnapshotId
        store.alters.alterTable(
            catalog,
            "ns",
            "t",
            ops = listOf(com.posthog.hoglake.model.AlterOp.AddColumn(ColumnDef("a", ColType.LONG))),
            expectedTableUuid = null,
            readSnapshot = read,
            author = "test",
            message = "fresh guarded alter",
        )
        // Two AddColumn ops in ONE alter: the second allocates from the first.
        store.alters.alterTable(
            catalog,
            "ns",
            "t",
            ops =
                listOf(
                    com.posthog.hoglake.model.AlterOp.AddColumn(ColumnDef("b", ColType.LONG)),
                    com.posthog.hoglake.model.AlterOp.AddColumn(ColumnDef("c", ColType.STRING)),
                ),
            expectedTableUuid = null,
            readSnapshot = null,
            author = "test",
            message = "two adds in one alter",
        )
        val cols = store.getTable(catalog, "ns", "t").columns.map { it.def.name }
        assertThat(cols).containsExactly("id", "a", "b", "c")
    }

    @Test
    fun `consumer id length is validated`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val t = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThatThrownBy { store.commitOffset(catalog, "", t.tableUuid, t.readSnapshotId) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { store.commitOffset(catalog, "x".repeat(129), t.tableUuid, t.readSnapshotId) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `the drop fence empties then deletes the tenant`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        // A non-empty catalog refuses, inside the fence transaction.
        assertThatThrownBy { store.dropCatalog(catalog, "test", "too early") }
            .isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        store.dropNamespace(catalog, "ns", author = "test", message = "empty it")
        store.dropCatalog(catalog, "test", "drop the empty catalog")
        // The tenant is gone; a direct read is the engine's 404.
        assertThatThrownBy { store.getCatalog(catalog) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        // And the name is free again.
        store.createCatalog(catalog, "s3://hoglake-it/$catalog")
    }

    @Test
    fun `table history excludes colliding namespace and view ids`() {
        val catalog = freshCatalog()
        val namespace = store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThat(namespace.namespaceId).isEqualTo(table.tableId)

        fun assertHistory(
            count: Long,
            earliest: Long?,
            at: Long? = null,
        ) {
            val summary = store.listTables(catalog, "ns", snapshot = at).single()
            assertThat(summary.snapshotCount).isEqualTo(count)
            assertThat(summary.earliestSnapshotId).isEqualTo(earliest)
        }
        assertHistory(1, table.readSnapshotId)
        val views = HogliteViews(store)
        val view = views.create(catalog, "ns", "v", "SELECT 1")
        assertThat(view.viewId).isEqualTo(table.tableId)
        assertHistory(1, table.readSnapshotId)
        views.drop(catalog, "ns", "v", author = "test", message = "drop the colliding view")
        assertHistory(1, table.readSnapshotId)
        val truncated =
            store.truncateTable(
                catalog,
                "ns",
                "t",
                table.tableUuid,
                author = "test",
                message = "truncate the table",
            ).snapshotId
        // Truncate records two table kinds at one snapshot.
        assertHistory(2, table.readSnapshotId)
        store.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://hoglake-it/$catalog/after.parquet", 2, 100)),
                        ),
                    ),
            ),
        )
        assertHistory(3, table.readSnapshotId)
        assertHistory(2, table.readSnapshotId, truncated)
        // Leave change rows below the floor to check the read bound independently of purge.
        scaleWrite(catalog, "UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1", truncated)
        assertThat(
            q(catalog, "SELECT COUNT(*) FROM changes WHERE snapshot_id < ?", truncated).firstRow()!![0].asLong(),
        ).isGreaterThan(0)
        assertHistory(2, truncated)
        assertHistory(1, truncated, truncated)
        store.createNamespace(catalog, "unrelated")
        val head = store.getCatalog(catalog).headSnapshotId
        scaleWrite(catalog, "UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1", head)
        assertHistory(0, null)
    }
}
