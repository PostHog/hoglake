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
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/** ASCII fixtures deliberately oppose names and UUIDs; no claim of cross-collation Unicode equivalence. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteReadParityIntegrationTest {
    private val db by lazy { PgTestSupport.freshDatabase() }
    private var pgStarted = false
    private val engine by lazy { HogliteConformanceEngine().also { engineStarted = true } }
    private var engineStarted = false
    private val json = jacksonObjectMapper()
    private var counter = 0
    private val issued = CopyOnWriteArrayList<com.fasterxml.jackson.databind.JsonNode>()

    @AfterAll
    fun close() {
        if (pgStarted) db.close()
        if (engineStarted) engine.close()
    }

    private fun fixture(
        lite: Boolean,
        body: (CatalogStore, AlterBackend, String) -> Unit,
    ) {
        // Resolve the optional engine on the JUnit thread before creating proxy resources.
        val engineUrl = if (lite) engine.url() else null
        val store: CatalogStore
        val alters: AlterBackend
        var proxy: HttpServer? = null
        var proxyClient: HogliteClient? = null
        val executor = Executors.newFixedThreadPool(4)
        if (engineUrl != null) {
            val http = HttpClient.newHttpClient()
            proxy =
                HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                    this.executor = executor
                    createContext("/") { ex ->
                        val raw = ex.requestBody.readBytes()
                        if (ex.requestURI.path.endsWith("/query")) issued.add(json.readTree(raw))
                        val builder =
                            HttpRequest.newBuilder(URI.create(engineUrl + ex.requestURI))
                                .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(raw))
                        ex.requestHeaders.getFirst("Content-Type")?.let { builder.header("Content-Type", it) }
                        val answer = http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray())
                        ex.sendResponseHeaders(answer.statusCode(), answer.body().size.toLong())
                        ex.responseBody.use { it.write(answer.body()) }
                    }
                    start()
                }
            proxyClient = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}", sleeper = {})
            val capturedStore = HogliteCatalogStore(proxyClient, HogliteConformanceEngine.TENANTS)
            store = capturedStore
            alters = HogliteAlters(capturedStore)
        } else {
            pgStarted = true
            store = PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi))
            alters = AlterService(db.jdbi)
        }
        try {
            run {
                val cat = "read-parity${counter++}"
                store.createCatalog(cat, "s3://hoglake-it/$cat")
                body(store, alters, cat)
            }
        } finally {
            proxyClient?.close()
            proxy?.stop(0)
            executor.shutdownNow()
        }
    }

    private fun write(
        lite: Boolean,
        cat: String,
        liteSql: String,
        pgSql: String,
    ) {
        if (lite) {
            engine.store.catalogOperation(cat) {
                engine.store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("fixture"),
                    null,
                    HogliteTx().apply { add(liteSql) },
                )
            }
        } else {
            db.jdbi.useHandle<Exception> {
                it.execute(pgSql.replace(":cat", "(SELECT catalog_id FROM hog_catalog WHERE name = '$cat')"))
            }
        }
    }

    @Test fun `PostgreSQL empty unsorted row ids omit bounds and sorted stored bounds survive`() = bounds(false)

    @Test fun `hoglite empty unsorted row ids omit bounds and sorted stored bounds survive`() = bounds(true)

    private fun bounds(lite: Boolean) =
        fixture(lite) { store, alters, cat ->
            store.createNamespace(cat, "ns")
            store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

            fun append(
                name: String,
                rows: Long,
                stats: List<ColumnStats>? = null,
            ) {
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
                                            "s3://hoglake-it/$cat/$name",
                                            rows,
                                            100,
                                            50,
                                            columnStats = stats,
                                        ),
                                    ),
                                ),
                            ),
                    ),
                )
            }
            val zeroStats = listOf(ColumnStats(1, 0, 0, null, null, null, null))
            append("positive", 1)
            append("positive-explicit", 1)
            append("zero-pending", 0)
            append("zero-provided", 0, zeroStats)
            append("zero-explicit", 0)
            write(
                lite,
                cat,
                "UPDATE files SET explicit_row_ids = 1 WHERE path LIKE '%explicit'",
                "UPDATE hog_data_file SET explicit_row_ids = true WHERE catalog_id = :cat AND path LIKE '%explicit'",
            )
            val files = store.listFiles(cat, "ns", "t").associateBy { it.path.substringAfterLast('/') }
            assertThat(files.getValue("positive").orderingBounds).isEqualTo(FileOrderingBounds.RowIds(0, 0))
            assertThat(files.getValue("positive-explicit").orderingBounds).isEqualTo(FileOrderingBounds.RowIds(1, null))
            for (name in listOf("zero-pending", "zero-provided", "zero-explicit")) {
                assertThat(files.getValue(name).rowIdStart).isEqualTo(2)
                assertThat(files.getValue(name).orderingBounds).describedAs(name).isNull()
            }
            val unsortedSnapshot = store.getCatalog(cat).headSnapshotId
            alters.alterTable(
                cat,
                "ns",
                "t",
                listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)))),
            )
            val sorted = store.listFiles(cat, "ns", "t").associateBy { it.path.substringAfterLast('/') }
            assertThat(sorted.getValue("zero-pending").orderingBounds).isNull()
            assertThat(
                sorted.getValue("zero-provided").orderingBounds,
            ).isInstanceOf(FileOrderingBounds.SortKey::class.java)
            val key = (sorted.getValue("zero-provided").orderingBounds as FileOrderingBounds.SortKey).column
            assertThat(key.fieldId).isEqualTo(1)
            assertThat(key.stats.lowerBound).isNull()
            assertThat(key.stats.upperBound).isNull()
            assertThat(
                store.listFiles(cat, "ns", "t", snapshot = unsortedSnapshot).single {
                    it.path.endsWith("positive")
                }.orderingBounds,
            )
                .isEqualTo(FileOrderingBounds.RowIds(0, 0))
        }

    @Test fun `PostgreSQL consumer names nulls and UUID ties determine grouped order but not offset order`() =
        consumers(
            false,
        )

    @Test fun `hoglite consumer names nulls and UUID ties determine grouped order but not offset order`() =
        consumers(
            true,
        )

    private fun consumers(lite: Boolean) =
        fixture(lite) { store, alters, cat ->
            if (!lite) {
                db.jdbi.useHandle<Exception> { h ->
                    println(
                        "POSTGRES_COLLATION " +
                            h.createQuery(
                                "SELECT datcollate, datctype, datlocprovider " +
                                    "FROM pg_database WHERE datname = current_database()",
                            ).mapToMap().one(),
                    )
                }
            }
            store.createNamespace(cat, "ans")
            store.createNamespace(cat, "zns")

            fun uuid(n: Int) = UUID.fromString("00000000-0000-0000-0000-%012d".format(n))

            fun table(
                ns: String,
                name: String,
                n: Int,
            ): UUID {
                val table = store.createTable(cat, ns, name, listOf(ColumnDef("id", ColType.LONG)))
                write(
                    lite,
                    cat,
                    "UPDATE tables SET table_uuid = '${uuid(n)}' WHERE id = ${table.tableId}",
                    "UPDATE hog_table SET table_uuid = '${uuid(
                        n,
                    )}' WHERE catalog_id = :cat AND table_id = ${table.tableId}",
                )
                return uuid(n)
            }
            table("zns", "a", 1)
            table("ans", "z", 2)
            table("ans", "a", 9)
            store.commitOffset(cat, "a-reader", uuid(9), 0)
            store.dropTable(cat, "ans", "a", author = "test", message = "drop fixture")
            table("ans", "a", 8)
            table("ans", "b", 3)
            alters.alterTable(cat, "ans", "b", listOf(AlterOp.RenameTable("c")))
            for (consumer in listOf("z-reader", "a-reader")) {
                for (n in listOf(1, 2, 3, 9, 8)) store.commitOffset(cat, consumer, uuid(n), 0)
                // Missing metadata is a deliberately seeded orphan, not a normal API operation.
                write(
                    lite,
                    cat,
                    "INSERT INTO offsets(consumer_id, table_uuid, committed_snapshot, updated_at) " +
                        "VALUES ('$consumer', '${uuid(0)}', 0, '2026-01-01T00:00:00Z')",
                    "INSERT INTO hog_consumer_offset(catalog_id, consumer_id, table_uuid, committed_snapshot) " +
                        "VALUES (:cat, '$consumer', '${uuid(0)}', 0)",
                )
            }
            val expected = listOf(8, 9, 3, 2, 1, 0).map(::uuid)
            issued.clear()
            val rows = store.listConsumers(cat)
            assertThat(
                rows.map { it.consumerId },
            ).containsExactlyElementsOf(List(6) { "a-reader" } + List(6) { "z-reader" })
            for (consumer in listOf("a-reader", "z-reader")) {
                val offsets = rows.filter { it.consumerId == consumer }
                assertThat(offsets.map { it.tableUuid }).containsExactlyElementsOf(expected)
                assertThat(offsets.map { it.namespace }).containsExactly("ans", "ans", "ans", "ans", "zns", null)
                assertThat(offsets.map { it.tableName }).containsExactly("a", "a", "c", "z", "a", null)
                assertThat(offsets.map { it.tableDropped }).containsExactly(false, true, false, false, false, false)
                assertThat(
                    store.listOffsets(cat, consumer).map {
                        it.tableUuid
                    },
                ).containsExactlyElementsOf((0..3).map(::uuid) + listOf(uuid(8), uuid(9)))
            }
            if (lite) {
                val query = issued.single { it["sql"].asText().contains("FROM offsets o LEFT JOIN") }
                println("CONSUMER_SQL_CAPTURE $query")
                assertThat(query["sql"].asText().trim().replace(Regex("\\s+"), " "))
                    .endsWith("ORDER BY o.consumer_id, t.namespace NULLS LAST, t.name NULLS LAST, o.table_uuid")
            }
            val empty = "$cat-empty"
            store.createCatalog(empty, "s3://hoglake-it/$empty")
        }
}
