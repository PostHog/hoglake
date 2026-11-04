package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/** Actual bound production statements, on the pinned external engine; no raised limits. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCommitCollectionsIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private var started = false
    private val json = jacksonObjectMapper()
    private val issued = CopyOnWriteArrayList<JsonNode>()

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    private fun fixture(body: (HogliteCatalogStore, String) -> Unit) {
        // Resolve the assumption on the JUnit thread, not in the proxy's handler.
        val running = engine
        started = true
        val http = HttpClient.newHttpClient()
        val executor = Executors.newFixedThreadPool(4)
        val proxy =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                this.executor = executor
                createContext("/") { ex ->
                    val raw = ex.requestBody.readBytes()
                    issued.add(json.readTree(raw))
                    val answer =
                        http.send(
                            HttpRequest.newBuilder(URI.create(running.url() + ex.requestURI))
                                .header("Content-Type", "application/json")
                                .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(raw)).build(),
                            HttpResponse.BodyHandlers.ofByteArray(),
                        )
                    ex.sendResponseHeaders(answer.statusCode(), answer.body().size.toLong())
                    ex.responseBody.use { it.write(answer.body()) }
                }
                start()
            }
        val client = HogliteClient(standardUrl = "http://127.0.0.1:${proxy.address.port}", sleeper = {})
        val store = HogliteCatalogStore(client).apply { requireOperationBindingForTests = true }
        try {
            val cat = "bind" + UUID.randomUUID().toString().replace("-", "")
            store.createCatalog(cat, "s3://hoglake-it/$cat")
            store.createNamespace(cat, "ns")
            store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            issued.clear()
            body(store, cat)
        } finally {
            client.close()
            proxy.stop(0)
            executor.shutdownNow()
        }
    }

    private fun statements(): List<JsonNode> =
        issued.flatMap { request ->
            request["statements"]?.toList() ?: if (request.has("sql")) listOf(request) else emptyList()
        }

    private fun paths(
        cat: String,
        n: Int,
        last: String,
    ): LinkedHashMap<String, String> =
        LinkedHashMap<String, String>().apply {
            repeat(n - 1) { put("s3://hoglake-it/$cat/unclaimed-$it", "data") }
            put(last, "data")
        }

    @Test
    fun `runtime boundary is 32766 including doubled and fixed binds and JSON preserves values`() =
        fixture { store, cat ->
            store.catalogOperation(cat) {
                assertThat(store.queryCatalog(cat, "SELECT sqlite_version()").firstRow()!![0]).isEqualTo("3.54.1")
                for ((n, arms, fixed) in listOf(
                    Triple(16_383, 2, 0),
                    Triple(16_384, 2, 0),
                    Triple(32_766, 1, 0),
                    Triple(32_767, 1, 0),
                    Triple(32_765, 1, 1),
                    Triple(32_766, 1, 1),
                )) {
                    val sql =
                        "SELECT COUNT(*) FROM meta WHERE " +
                            (1..arms).joinToString(" AND ") { "0 IN (${(1..n).joinToString(",") { "?" }})" } +
                            if (fixed == 1) " AND id = ?" else ""
                    val args = List<Any?>(n * arms + fixed) { 0L }
                    if (args.size <= 32_766) {
                        assertThat(store.queryCatalog(cat, sql, *args.toTypedArray()).firstRow()!![0].asLong())
                            .isEqualTo(if (fixed == 1) 0L else 1L)
                        println("REAL_BIND_BOUNDARY binds=${args.size} accepted n=$n arms=$arms fixed=$fixed")
                    } else {
                        assertThatThrownBy { store.queryCatalog(cat, sql, *args.toTypedArray()) }
                            .hasMessageContaining("too many SQL variables")
                        println("REAL_BIND_BOUNDARY binds=${args.size} refused=too_many_SQL_variables")
                    }
                }
                val values = listOf<Any?>("a'\"\\\n雪?;--", null, "null", 9_007_199_254_740_993L, 7L, 7L)
                val rows =
                    store.queryCatalog(
                        cat,
                        "SELECT value, type FROM json_each(?) ORDER BY key",
                        json.writeValueAsString(values),
                    ).rows
                assertThat(
                    rows.map { if (it[0] is Number) it[0].asLong() else it[0] },
                ).containsExactlyElementsOf(values)
                assertThat(rows.map { it[1] }).containsExactly("text", "null", "text", "integer", "integer", "integer")
                assertThat(store.queryCatalog(cat, "SELECT value FROM json_each(?)", "[]").rows).isEmpty()
            }
        }

    @Test
    fun `fallback claims cross old doubled boundary and keep tail claim identity and retained kind`() =
        fixture { store, cat ->
            val owner = UUID.randomUUID()
            val active = store.uploads.claim(cat, UUID.randomUUID(), owner, "s3://hoglake-it/$cat", "data")
            store.catalogOperation(cat) {
                assertThat(HogliteCommit.decideClaims(store, cat, emptyMap(), owner)).isEmpty()
                for (n in listOf(16_383, 16_384, 32_766, 32_767)) {
                    val result =
                        runCatching { HogliteCommit.decideClaims(store, cat, paths(cat, n, active.path), owner) }
                    assertThat(result.exceptionOrNull()).describedAs("actual fallback claims at $n paths").isNull()
                    assertThat(result.getOrThrow()).containsExactly(active.uploadId.toString())
                }
                assertThatThrownBy {
                    HogliteCommit.decideClaims(store, cat, paths(cat, 32_767, active.path), UUID.randomUUID())
                }.isInstanceOf(HoglakeException.CommitConflict::class.java)
            }
            store.commit(
                cat,
                CommitRequest(
                    idempotencyKey = owner,
                    appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(active.path, 1, 100, 50)))),
                ),
            )
            store.catalogOperation(cat) {
                val retained =
                    runCatching { HogliteCommit.decideClaims(store, cat, paths(cat, 32_767, active.path), owner) }
                assertThat(retained.exceptionOrNull()).describedAs("registered retained same-kind tail path").isNull()
                assertThat(retained.getOrThrow()).isEmpty()
                assertThatThrownBy { HogliteCommit.decideClaims(store, cat, mapOf(active.path to "delete"), owner) }
                    .isInstanceOf(HoglakeException.CommitConflict::class.java)
            }
        }

    @Test
    fun `large actual PlanReads collection reaches file validation instead of variable overflow`() =
        fixture { store, cat ->
            val files =
                (1..32_767).map { n ->
                    FileRegistration("s3://hoglake-it/$cat/$n", if (n == 32_767) -1 else 1, 100, 50)
                }
            assertThatThrownBy {
                store.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", files))))
            }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("record_count")
            assertThat(statements().filter { it["sql"].asText().startsWith("INSERT INTO files") }).isEmpty()
            assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(2L)
        }

    @Test
    fun `every captured production collection uses constant binds indexed relations and atomic final guards`() =
        fixture { store, cat ->
            val owner = UUID.randomUUID()
            val claim =
                store.uploads.claim(
                    cat,
                    UUID.randomUUID(),
                    owner,
                    "s3://hoglake-it/$cat/a'();--%22%5C%E9%9B%AA",
                    "data",
                )
            val sensitive = claim.path
            store.commit(
                cat,
                CommitRequest(
                    idempotencyKey = owner,
                    appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(sensitive, 10, 100, 50)))),
                ),
            )
            store.catalogOperation(cat) {
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("fixture"),
                    null,
                    HogliteTx().apply { add("UPDATE files SET id=9007199254740993 WHERE id=1") },
                )
            }
            val target = store.listFiles(cat, "ns", "t").single().dataFileId
            assertThat(target).isEqualTo(9_007_199_254_740_993L)
            assertThat(store.listFiles(cat, "ns", "t").single().path).isEqualTo(sensitive)
            val read = store.getCatalog(cat).headSnapshotId
            val dvOwner = UUID.randomUUID()
            val dvClaim = store.uploads.claim(cat, UUID.randomUUID(), dvOwner, "s3://hoglake-it/$cat/dv", "delete")
            issued.clear()
            val result =
                store.commit(
                    cat,
                    CommitRequest(
                        readSnapshot = read,
                        requireUnchangedTables = true,
                        idempotencyKey = dvOwner,
                        appends =
                            listOf(
                                TableAppend(
                                    "ns",
                                    "t",
                                    listOf(FileRegistration("s3://hoglake-it/$cat/new", 1, 100, 50)),
                                ),
                            ),
                        deletes =
                            listOf(
                                TableDeletes(
                                    "ns",
                                    "t",
                                    listOf(DeleteFileRegistration(target, dvClaim.path, 1, 10)),
                                ),
                            ),
                    ),
                )
            assertThat(result.snapshotId).isEqualTo(read + 1)
            val all = statements()
            val claims = all.single { it["sql"].asText().startsWith("SELECT upload_id, path") }
            val retained = all.single { it["sql"].asText().startsWith("SELECT path, 0") }
            val conflicts = all.single { it["sql"].asText().startsWith("SELECT DISTINCT object_id") }
            val durable =
                all.single {
                    it["sql"].asText().startsWith(
                        "SELECT f.id, f.table_id, f.partition_values_json",
                    )
                }
            val content =
                all.single {
                    val sql = it["sql"].asText()
                    sql.startsWith("SELECT COUNT(*) FROM changes") && "object_id IN" in sql
                }
            val removal = all.single { it["sql"].asText().startsWith("SELECT COUNT(*) FROM removals") }
            val collections = listOf(claims, retained, conflicts, durable, content, removal)
            assertThat(collections.map { it["args"].size() }).containsExactly(1, 2, 3, 1, 2, 1)
            assertThat(collections.all { "json_each(?)" in it["sql"].asText() }).isTrue()
            assertThat(json.readTree(durable["args"][0].asText()).map { it.asLong() }).containsExactly(target)
            val write =
                issued.single {
                        req ->
                    req["statements"]?.any { it["sql"].asText().startsWith("INSERT INTO files") } == true
                }
            assertThat(write["statements"].toList()).contains(content, removal)
            assertThat(write["statements"].any { it["sql"].asText().startsWith("INSERT INTO receipts") }).isTrue()
            val readBatch = issued.single { req -> req["statements"]?.any { it == claims } == true }
            assertThat(readBatch["statements"].toList()).contains(claims, retained, conflicts, durable)
            assertThat(readBatch["txid"].asText()).startsWith("read:")
            store.createNamespace(cat, "other")
            store.createTable(cat, "other", "t", listOf(ColumnDef("id", ColType.LONG)))
            store.createTable(cat, "ns", "decoy", listOf(ColumnDef("id", ColType.LONG)))
            store.alters.alterTable(cat, "other", "t", listOf(AlterOp.AddColumn(ColumnDef("extra", ColType.LONG))))
            store.alters.alterTable(cat, "ns", "decoy", listOf(AlterOp.AddColumn(ColumnDef("extra", ColType.LONG))))
            val manyPaths = json.writeValueAsString(paths(cat, 32_767, sensitive).keys + dvClaim.path)
            val manyIds = json.writeValueAsString(List(32_766) { -1L } + target)
            val names = json.writeValueAsString(List(16_383) { listOf("other", "decoy") } + listOf(listOf("ns", "t")))
            // Replay actual SQL, changing ONLY the collection arguments. Counts, not result materialization.
            val probes =
                listOf(
                    claims to listOf(manyPaths),
                    retained to listOf(manyPaths, manyPaths),
                    conflicts to listOf(0L, names, names),
                    durable to listOf(manyIds),
                    content to listOf(0L, json.writeValueAsString(List(32_766) { -1L } + 1L)),
                    removal to listOf(manyPaths),
                )
            store.catalogOperation(cat) {
                for ((statement, args) in probes) {
                    val sql = statement["sql"].asText()
                    val rows = store.queryCatalog(cat, sql, *args.toTypedArray()).rows
                    if (statement === durable) assertThat(rows.map { it[0].asLong() }).containsExactly(target)
                    if (statement === claims) {
                        assertThat(rows.map { it[0].asString() }).containsExactlyInAnyOrder(
                            claim.uploadId.toString(),
                            dvClaim.uploadId.toString(),
                        )
                        assertThat(rows.map { it[3].asString() }).containsOnly("registered")
                    }
                    if (statement === retained) {
                        assertThat(rows.map { it[0].asString() to it[1].asLong() }).containsExactlyInAnyOrder(
                            sensitive to 0L,
                            dvClaim.path to 1L,
                        )
                    }
                    if (statement === conflicts) {
                        assertThat(rows.map { it[0].asLong() }.toSet()).containsExactly(1L)
                    }
                    if (statement === content) assertThat(rows.single()[0].asLong()).isGreaterThan(0L)
                    val plan =
                        store.queryCatalog(cat, "EXPLAIN QUERY PLAN $sql", *args.toTypedArray()).rows
                            .joinToString("\n") { it.last().asString() }
                    println("REAL_COLLECTION_EQP $sql\n$plan")
                    val indexes =
                        when (statement) {
                            claims -> listOf("uploads_path")
                            retained -> listOf("files_path", "dvs_path", "uploads_path")
                            content, conflicts -> listOf("changes_obj")
                            removal -> listOf("removals_undrained_path")
                            else -> listOf("INTEGER PRIMARY KEY")
                        }
                    for (index in indexes) assertThat(plan).contains(index)
                }
                // Use the actual final guard with a collision at the tail, after a write:
                // the write MUST roll back, not be protected by a stale pre-read.
                val setup = HogliteTx()
                setup.add(
                    "INSERT INTO removals(path,file_kind,queued_snapshot,queued_at) VALUES(?,'data',0,?)",
                    sensitive,
                    "2026-11-04T00:00:00Z",
                )
                store.runTx(cat, HogliteCatalogStore.freshTxid("fixture"), null, setup)
                val head = store.getCatalog(cat).headSnapshotId
                val tx = HogliteTx()
                tx.add("UPDATE meta SET next_snapshot = next_snapshot + 1")
                tx.expectZero(removal["sql"].asText(), manyPaths) { HoglakeException.CommitConflict("tail collision") }
                assertThatThrownBy { store.runTx(cat, HogliteCatalogStore.freshTxid("guard"), null, tx) }
                    .isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessage("tail collision")
                assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(head)
            }
        }

    @Test
    fun `commit HTTP preserves duplicate spans replay and atomic removal refusal with JSON guards`() =
        fixture { store, cat ->
            val cfg =
                engine.config().copy(
                    database = DatabaseBackend.HOGLITE,
                    hogliteUrl = engine.url(),
                    hydratorIntervalMs = 0,
                    expiryIntervalMs = 0,
                    cleanupIntervalMs = 0,
                    compactionIntervalMs = 0,
                    retirementIntervalMs = 0,
                )
            App.buildHoglite(cfg, store, HealthProbe.forProbe(1000) { true }).use { app ->
                val owner = UUID.randomUUID()
                val claim = store.uploads.claim(cat, UUID.randomUUID(), owner, "s3://hoglake-it/$cat/a';--", "data")

                fun payload(key: UUID) =
                    json.writeValueAsString(
                        mapOf(
                            "idempotency_key" to key.toString(),
                            "appends" to
                                listOf(
                                    mapOf(
                                        "namespace" to "ns", "table" to "t",
                                        "files" to
                                            listOf(1, 2).map {
                                                mapOf(
                                                    "path" to claim.path, "record_count" to "$it",
                                                    "file_size_bytes" to "100", "footer_size" to "50",
                                                )
                                            },
                                    ),
                                ),
                        ),
                    )
                testApplication {
                    application { app.module(this) }
                    val body = payload(owner)
                    val first =
                        client.post("/v1/catalogs/$cat/commit") {
                            contentType(ContentType.Application.Json)
                            setBody(body)
                        }
                    assertThat(first.status).isEqualTo(HttpStatusCode.OK)
                    val replay =
                        client.post("/v1/catalogs/$cat/commit") {
                            contentType(ContentType.Application.Json)
                            setBody(body)
                        }
                    assertThat(replay.status).isEqualTo(HttpStatusCode.OK)
                    assertThat(replay.bodyAsText()).isEqualTo(first.bodyAsText())
                    val files = store.listFiles(cat, "ns", "t").sortedBy { it.rowIdStart }
                    assertThat(files.map { it.path }).containsExactly(claim.path, claim.path)
                    assertThat(files.map { it.rowIdStart }).containsExactly(0L, 1L)
                    assertThat(files.map { it.recordCount }).containsExactly(1L, 2L)
                    val head = store.getCatalog(cat).headSnapshotId
                    store.catalogOperation(cat) {
                        store.runTx(
                            cat,
                            HogliteCatalogStore.freshTxid("fixture"),
                            null,
                            HogliteTx().apply {
                                add(
                                    "INSERT INTO removals(path,file_kind,queued_snapshot,queued_at) " +
                                        "VALUES(?,'data',0,?)",
                                    claim.path,
                                    "2026-11-04T00:00:00Z",
                                )
                            },
                        )
                    }
                    val refused =
                        client.post("/v1/catalogs/$cat/commit") {
                            contentType(ContentType.Application.Json)
                            setBody(payload(UUID.randomUUID()))
                        }
                    assertThat(refused.status).isEqualTo(HttpStatusCode.Conflict)
                    assertThat(json.readTree(refused.bodyAsText())["error"].asText()).isEqualTo("commit_conflict")
                    assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(head)
                    assertThat(store.listFiles(cat, "ns", "t")).hasSize(2)
                }
            }
        }

    @Test
    fun `JSON collections do not reset the cumulative read transaction result budget`() =
        fixture { store, cat ->
            val a = json.writeValueAsString(List(60_000) { 1 })
            val b = json.writeValueAsString(List(40_001) { 2 })
            store.catalogOperation(cat) {
                val outcome =
                    runCatching {
                        store.readCatalog(
                            cat,
                            listOf(
                                HogliteStatement("SELECT value FROM json_each(?)", listOf(a)),
                                HogliteStatement("SELECT value FROM json_each(?)", listOf(b)),
                            ),
                        )
                    }
                assertThat(outcome.isFailure).isTrue()
                assertThat(outcome.exceptionOrNull()).hasMessageContaining("result_too_large")
                println("REAL_CUMULATIVE_BUDGET 60000 + 40001 rows refused: ${outcome.exceptionOrNull()!!.message}")
            }
        }
}
