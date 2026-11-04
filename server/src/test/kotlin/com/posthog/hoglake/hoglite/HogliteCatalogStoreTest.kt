package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * The catalog lifecycle of [HogliteCatalogStore] (create, list, drop and
 * the drop fence) against an in-process fake of hoglite's HTTP API. The
 * fake keeps only the state these paths read: which tenants exist, each
 * tenant's meta row, and its drop fence. It evaluates the guards these
 * paths send by their SQL text. No hoglite binary and no Docker.
 */
class HogliteCatalogStoreTest {
    private val json = jacksonObjectMapper()

    private class Tenant {
        var name: String? = null
        var dataPath: String? = null
        var dropping = false
    }

    private val tenants = ConcurrentHashMap<String, Tenant>()

    /** One entry per request: "METHOD path", plus the first SQL statement of a /tx. */
    private val requests = CopyOnWriteArrayList<String>()

    /** A fence guard whose SQL contains this text counts this many rows (the catalog is not empty). */
    private var nonEmpty: Pair<String, Long>? = null

    /** The storage-usage read's row: file bytes, DV bytes, undrained removals. */
    private var usage: List<Long> = listOf(0, 0, 0)

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex -> handle(ex) }
            start()
        }

    private val client = HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {})

    private val resolved = AtomicInteger()

    /** Incarnation-style ids: the store must use exactly what the resolver names. */
    private val store =
        HogliteCatalogStore(client, { catalog ->
            resolved.incrementAndGet()
            "t-$catalog-1"
        })

    @AfterEach
    fun stop() {
        server.stop(0)
    }

    private fun handle(ex: HttpExchange) {
        val path = ex.requestURI.path
        val body = ex.requestBody.readBytes().takeIf { it.isNotEmpty() }?.let { json.readTree(it) }
        val tenantId = path.removePrefix("/v1/t/").removePrefix("/v1/tenants/").substringBefore('/')
        val first = body?.get("statements")?.get(0)?.get("sql")?.asText()
        requests += "${ex.requestMethod} $path" + (first?.let { " :: $it" } ?: "")
        val (status, answer) =
            when {
                ex.requestMethod == "PUT" -> {
                    tenants.putIfAbsent(tenantId, Tenant())
                    200 to mapOf("ok" to true)
                }
                ex.requestMethod == "DELETE" -> {
                    tenants.remove(tenantId)
                    200 to mapOf("ok" to true)
                }
                path == "/v1/query-batch" -> 200 to mapOf("results" to body!!["queries"].map { batchItem(it) })
                path.endsWith("/release") -> 200 to mapOf("ok" to true)
                tenants[tenantId] == null -> 404 to mapOf("error" to "tenant does not exist")
                path.endsWith("/query") -> 200 to query(tenants[tenantId]!!, body!!["sql"].asText())
                path.endsWith("/tx") -> tx(tenants[tenantId]!!, body!!["statements"])
                else -> 400 to mapOf("error" to "unexpected $path")
            }
        val bytes = json.writeValueAsBytes(answer)
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    private fun result(vararg rows: List<Any?>): Map<String, Any?> =
        mapOf("cols" to List(rows.firstOrNull()?.size ?: 0) { "c$it" }, "rows" to rows.toList(), "affected" to 0)

    private fun metaRow(t: Tenant): List<Any?> = listOf(t.name, t.dataPath, 0, 0, 0, null, null, 0)

    private fun query(
        t: Tenant,
        sql: String,
    ): Map<String, Any?> =
        when {
            sql.startsWith("SELECT name, data_path,") -> if (t.name == null) result() else result(metaRow(t))
            sql == HogliteCatalogStore.STORAGE_USAGE_SQL -> result(usage)
            // The count a CatalogNotEmpty message reports.
            else -> result(listOf(nonEmpty?.second ?: 0))
        }

    private fun batchItem(q: JsonNode): Map<String, Any?> {
        val t =
            tenants[q["tenant"].asText()]
                ?: return mapOf("status" to 404, "body" to mapOf("error" to "tenant does not exist"))
        return mapOf("status" to 200, "body" to query(t, q["sql"].asText()))
    }

    /** The value a guard's COUNT(*) returns on [t]. */
    private fun count(
        t: Tenant,
        sql: String,
    ): Long =
        when {
            sql.contains("sqlite_master") && sql.contains("'meta'") -> if (t.name == null) 0 else 1
            sql.contains("dropping = 0") -> if (t.dropping) 0 else 1
            nonEmpty != null && sql.contains(nonEmpty!!.first) -> nonEmpty!!.second
            else -> -1 // "passes": the statement's own expectation
        }

    private fun tx(
        t: Tenant,
        statements: JsonNode,
    ): Pair<Int, Map<String, Any?>> {
        for ((i, st) in statements.withIndex()) {
            val expected = st["expect_scalar_int"]?.asLong() ?: continue
            val actual = count(t, st["sql"].asText()).takeIf { it >= 0 } ?: expected
            if (actual != expected) {
                val error = "expect_failed: expectation failed at statement $i"
                return 409 to mapOf("error" to error, "guard_index" to i + 1)
            }
        }
        for (st in statements) {
            val sql = st["sql"].asText()
            if (sql.startsWith("INSERT INTO meta")) {
                t.name = st["args"][0].asText()
                t.dataPath = st["args"][1].asText()
            }
            if (sql == "UPDATE meta SET dropping = 1 WHERE id = 1") t.dropping = true
        }
        return 200 to mapOf("results" to statements.map { result() })
    }

    private fun seed(
        tenantId: String,
        name: String,
        dataPath: String = "s3://bucket/$name",
    ) {
        tenants[tenantId] =
            Tenant().apply {
                this.name = name
                this.dataPath = dataPath
            }
    }

    @Test
    fun `create bootstraps the tenant the resolver names in one guarded transaction`() {
        val info = store.createCatalog("wh", "s3://bucket/wh", "me", "hello")

        assertThat(info.name).isEqualTo("wh")
        assertThat(info.dataPath).isEqualTo("s3://bucket/wh")
        assertThat(info.headSnapshotId).isZero()
        assertThat(tenants.keys).containsExactly("t-wh-1")
        assertThat(requests[0]).isEqualTo("PUT /v1/tenants/t-wh-1")
        // The duplicate guard is statement 0, before any DDL.
        assertThat(requests[1]).startsWith("POST /v1/t/t-wh-1/tx :: SELECT COUNT(*) FROM sqlite_master")
        // The read-back of the new catalog reuses the operation's binding.
        assertThat(resolved.get()).isEqualTo(1)
    }

    @Test
    fun `a create on a tenant that already holds a catalog is AlreadyExists and changes nothing`() {
        seed("t-wh-1", "wh", "s3://bucket/old")

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/new", null, null) }

        assertThat(failure).isInstanceOf(HoglakeException.AlreadyExists::class.java).hasMessageContaining("'wh'")
        assertThat(tenants["t-wh-1"]!!.dataPath).isEqualTo("s3://bucket/old")
    }

    @Test
    fun `create refuses an invalid name or data path before any request`() {
        for ((name, path) in listOf("Bad" to "s3://bucket/x", "ok" to "s3://", "ok" to "s3://b/../x", "ok" to "/tmp")) {
            assertThat(catchThrowable { store.createCatalog(name, path, null, null) })
                .`as`("$name $path")
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(requests).isEmpty()
    }

    @Test
    fun `create accepts a numeric-leading name as Postgres does since V13`() {
        val name = "0e9b1c52-7f3a-4d6e-9a41-2b8c5d7e1f60"

        assertThat(store.createCatalog(name, "s3://bucket/u", null, null).name).isEqualTo(name)
    }

    @Test
    fun `storage usage counts every file and DV row and only undrained removals`() {
        seed("t-wh-1", "wh")
        usage = listOf(100, 20, 3)

        val footprint = store.storageUsage("wh")

        assertThat(footprint.trackedBytes).isEqualTo(120)
        assertThat(footprint.pendingDeleteFiles).isEqualTo(3)
        // StorageUsageRepo's contract: ended rows still occupy storage.
        assertThat(HogliteCatalogStore.STORAGE_USAGE_SQL)
            .doesNotContain("end_snapshot")
            .contains("FROM removals WHERE drained_at IS NULL")
    }

    @Test
    fun `storage usage refuses a byte total that overflows`() {
        seed("t-wh-1", "wh")
        usage = listOf(Long.MAX_VALUE, 1, 0)

        assertThat(catchThrowable { store.storageUsage("wh") }).isInstanceOf(ArithmeticException::class.java)
    }

    @Test
    fun `get of a catalog whose tenant does not exist is NotFound`() {
        assertThat(catchThrowable { store.getCatalog("gone") })
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("'gone'")
    }

    @Test
    fun `list reads the named catalogs in one batch and skips missing or foreign tenants`() {
        seed("t-a-1", "a")
        seed("t-b-1", "b")
        seed("t-d-1", "a") // a tenant whose meta names another catalog

        val listed = store.listCatalogs(listOf("b", "a", "c", "d", "a"))

        assertThat(listed.map { it.name }).containsExactly("b", "a")
        assertThat(listed.map { it.dataPath }).containsExactly("s3://bucket/b", "s3://bucket/a")
        assertThat(requests).containsExactly("POST /v1/query-batch")
    }

    @Test
    fun `drop fences the catalog, then releases and deletes its tenant`() {
        seed("t-wh-1", "wh")

        val dropped = store.dropCatalog("wh", null, null)

        assertThat(dropped.name).isEqualTo("wh")
        assertThat(dropped.dataPath).isEqualTo("s3://bucket/wh")
        assertThat(tenants).isEmpty()
        val writes = requests.filterNot { it.contains("/query") }
        assertThat(writes).hasSize(3)
        assertThat(writes[0]).startsWith("POST /v1/t/t-wh-1/tx :: SELECT COUNT(*) FROM namespaces")
        assertThat(writes[1]).isEqualTo("POST /v1/t/t-wh-1/release")
        assertThat(writes[2]).isEqualTo("DELETE /v1/tenants/t-wh-1")
    }

    @Test
    fun `drop of a catalog whose tenant does not exist is NotFound and deletes nothing`() {
        assertThat(catchThrowable { store.dropCatalog("gone", null, null) })
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(requests.filter { it.startsWith("DELETE") || it.endsWith("/release") }).isEmpty()
    }

    @Test
    fun `drop of a catalog that is not empty names the reason and keeps the tenant`() {
        seed("t-wh-1", "wh")
        nonEmpty = "FROM offsets" to 2L

        val failure = catchThrowable { store.dropCatalog("wh", null, null) }

        assertThat(failure).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        assertThat((failure as HoglakeException.CatalogNotEmpty).reason)
            .isEqualTo(CatalogNotEmptyReason.CONSUMER_OFFSETS)
        assertThat(failure).hasMessageContaining("2 consumer offset(s) remain")
        assertThat(tenants["t-wh-1"]!!.dropping).isFalse()
        assertThat(requests.filter { it.startsWith("DELETE") || it.endsWith("/release") }).isEmpty()
    }

    @Test
    fun `a write after the drop fence is NotFound because the catalog is being dropped`() {
        seed("t-wh-1", "wh")
        tenants["t-wh-1"]!!.dropping = true

        val failure = catchThrowable { store.createNamespace("wh", "ns", null, null) }

        assertThat(failure).isInstanceOf(HoglakeException.NotFound::class.java).hasMessageContaining("being dropped")
    }
}
