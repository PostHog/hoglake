package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * `listCatalogs` costs a CONSTANT number of hoglite round trips, not two
 * per catalog, and stays bound to the incarnation the registry named.
 *
 * Before: one registry read for the names, then per catalog a registry
 * `SELECT tenant ... WHERE name = ?` and a meta read (plus eight migration
 * probes the first time a process saw the tenant). 468 catalogs were ~940
 * sequential reads: 0.6 ms each on the registry's owner, ~30 ms each
 * proxied, ~170 ms each when the tenants had been released.
 *
 * hoglite is faked at the HTTP level so every request is counted exactly;
 * the same counts against the real engine are in
 * HogliteBackendIntegrationTest (`a list of N catalogs ...`).
 */
class HogliteCatalogListTest {
    private val servers = mutableListOf<HttpServer>()

    @AfterEach
    fun stop() = servers.forEach { it.stop(0) }

    private val json = jacksonObjectMapper()

    private val registry = "hoglake-__hoglake_registry"

    private data class Meta(
        val name: String,
        val dataPath: String,
        val head: Long = 3,
    )

    /**
     * The registry tenant and the catalog tenants as hoglite serves them to
     * a list: `/query` on each, and `/v1/query-batch` (unless
     * [batchSupported] is off, when the route answers like Go's mux does
     * for an unknown path: a plain-text 404). Counts every request.
     */
    private inner class FakeEngine {
        /** name -> tenant id, as the registry's `catalogs` rows. */
        val catalogs = ConcurrentHashMap<String, String>()

        /** tenant id -> its meta row; a tenant not here answers 404. */
        val tenants = ConcurrentHashMap<String, Meta>()

        @Volatile var batchSupported = true

        /** Tenants whose batch item answers 503 (a transient failure); their own /query answers. */
        val flakyInBatch: MutableSet<String> = ConcurrentHashMap.newKeySet()

        /** Whale-class tenants: the batch (standard pool) answers them 421 `class: whale`. */
        val whales: MutableSet<String> = ConcurrentHashMap.newKeySet()

        /** Runs after registry rows are captured but before the response: a concurrent drop/recreate lands here. */
        @Volatile var afterRegistryRead: () -> Unit = {}

        val requests = AtomicInteger()
        val registryReads = AtomicInteger()
        val tenantReads = AtomicInteger()
        val batches = AtomicInteger()
        val batchItems = AtomicInteger()
        val tenantsRead: MutableSet<String> = ConcurrentHashMap.newKeySet()

        fun add(
            name: String,
            tenant: String = "t-$name",
            dataPath: String = "s3://b/$name",
        ) {
            tenants[tenant] = Meta(name, dataPath)
            catalogs[name] = tenant
        }

        fun handle(ex: HttpExchange) {
            requests.incrementAndGet()
            val path = ex.requestURI.path
            val raw = ex.requestBody.readBytes()
            val body = if (raw.isEmpty()) emptyMap() else json.readValue<Map<String, Any?>>(raw)
            when {
                path == "/v1/t/$registry/query" -> {
                    registryReads.incrementAndGet()
                    // The list's one read, plus the shapes a per-catalog
                    // list made (the names, then each name's tenant), so a
                    // regression fails on the COUNT, not on this fake.
                    val sql = body["sql"] as String
                    val args = (body["args"] as? List<*>).orEmpty()
                    val sorted = catalogs.entries.sortedBy { it.key }
                    val rows =
                        when (sql) {
                            "SELECT name, tenant FROM catalogs ORDER BY name LIMIT ?" ->
                                sorted.take((args[0] as Number).toInt()).map { listOf(it.key, it.value) }
                            "SELECT name, tenant FROM catalogs WHERE name > ? ORDER BY name LIMIT ?" ->
                                sorted.filter { it.key > args[0] as String }
                                    .take((args[1] as Number).toInt())
                                    .map { listOf(it.key, it.value) }
                            "SELECT name FROM catalogs ORDER BY name" -> sorted.map { listOf(it.key) }
                            "SELECT tenant FROM catalogs WHERE name = ?" ->
                                listOfNotNull(catalogs[args[0]]?.let { listOf(it) })
                            else -> error("unexpected registry read: $sql")
                        }
                    val response = mapOf("cols" to List(rows.firstOrNull()?.size ?: 1) { "c$it" }, "rows" to rows)
                    afterRegistryRead()
                    reply(ex, 200, response)
                }
                path == "/v1/query-batch" -> {
                    if (!batchSupported) return reply(ex, 404, "404 page not found\n")
                    batches.incrementAndGet()

                    @Suppress("UNCHECKED_CAST")
                    val queries = body["queries"] as List<Map<String, Any?>>
                    batchItems.addAndGet(queries.size)
                    val results =
                        queries.map { q ->
                            val tenant = q["tenant"] as String
                            if (tenant in flakyInBatch) {
                                mapOf("status" to 503, "body" to mapOf("error" to "busy"))
                            } else if (tenant in whales) {
                                // The standard pool's answer for a whale-class tenant.
                                mapOf("status" to 421, "body" to mapOf("error" to "wrong class", "class" to "whale"))
                            } else {
                                read(tenant, q["sql"] as String)
                            }
                        }
                    reply(ex, 200, mapOf("results" to results))
                }
                path.startsWith("/v1/t/") && path.endsWith("/query") -> {
                    tenantReads.incrementAndGet()
                    val tenant = path.removePrefix("/v1/t/").removeSuffix("/query")
                    if (tenant in whales) return reply(ex, 421, mapOf("error" to "wrong class", "class" to "whale"))
                    val sql = body["sql"] as String
                    if (sql.startsWith("SELECT (SELECT COUNT(*) FROM sqlite_master")) {
                        // A first touch's migration probes: every migration applied.
                        val n = sql.split("(SELECT COUNT(*)").size - 1
                        return reply(ex, 200, mapOf("cols" to List(n) { "c$it" }, "rows" to listOf(List(n) { 1 })))
                    }
                    val r = read(tenant, sql)
                    reply(ex, r["status"] as Int, r["body"]!!)
                }
                else -> reply(ex, 500, mapOf("error" to "unexpected ${ex.requestMethod} $path"))
            }
        }

        /** One tenant's answer to the list's meta read: `{status, body}` exactly as /query would answer. */
        private fun read(
            tenant: String,
            sql: String,
        ): Map<String, Any?> {
            tenantsRead += tenant
            check(sql.startsWith("SELECT name, data_path") && sql.endsWith("FROM meta WHERE id = 1")) {
                "unexpected tenant read: $sql"
            }
            val m =
                tenants[tenant]
                    ?: return mapOf("status" to 404, "body" to mapOf("error" to "tenant does not exist"))
            val row = listOf(m.name, m.dataPath, m.head, 1, 0, null, null, 1)
            return mapOf("status" to 200, "body" to mapOf("cols" to List(row.size) { "c$it" }, "rows" to listOf(row)))
        }

        fun reset() {
            listOf(requests, registryReads, tenantReads, batches, batchItems).forEach { it.set(0) }
            tenantsRead.clear()
        }
    }

    private fun reply(
        ex: HttpExchange,
        status: Int,
        body: Any,
    ) {
        val bytes = if (body is String) body.toByteArray() else json.writeValueAsBytes(body)
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    private fun serve(fake: FakeEngine): String {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newFixedThreadPool(16)
        server.createContext("/") { ex -> fake.handle(ex) }
        server.start()
        servers += server
        return "http://127.0.0.1:${server.address.port}"
    }

    private fun store(fake: FakeEngine) =
        HogliteCatalogStore(HogliteClient(standardUrl = serve(fake), retryBudgetMs = 2_000, sleeper = {}))

    // ---- round trips -------------------------------------------------------

    @Test
    fun `a list of N catalogs costs one registry read and one batch per chunk, not two reads per catalog`() {
        val fake = FakeEngine()
        val n = 600
        (0 until n).forEach { fake.add("c%04d".format(it)) }
        val store = store(fake)

        val listed = store.listCatalogs(null)

        assertThat(listed.map { it.name }).containsExactlyElementsOf((0 until n).map { "c%04d".format(it) })
        assertThat(listed.first().dataPath).isEqualTo("s3://b/c0000")
        assertThat(listed.first().headSnapshotId).isEqualTo(3)
        // 1 registry read + ceil(600 / 250) batches. Not 2N + 1 = 1201.
        assertThat(fake.registryReads.get()).isEqualTo(1)
        assertThat(fake.batches.get()).isEqualTo(3)
        assertThat(fake.batchItems.get()).isEqualTo(n)
        assertThat(fake.tenantReads.get()).isEqualTo(0)
        assertThat(fake.requests.get()).isEqualTo(4)

        // Under one chunk, a list is exactly two round trips, whatever N is.
        fake.reset()
        store.listBatchSize = n
        store.listCatalogs(null)
        assertThat(fake.requests.get()).isEqualTo(2)
    }

    @Test
    fun `the registry is read in pages under hoglite's row cap, each page one round trip`() {
        val fake = FakeEngine()
        (0 until 250).forEach { fake.add("c%04d".format(it)) }
        val store = store(fake).apply { registryPageSize = 100 }

        assertThat(
            store.listCatalogs(null).map { it.name },
        ).containsExactlyElementsOf((0 until 250).map { "c%04d".format(it) })
        // Pages of 100, 100, 50: the short page ends it.
        assertThat(fake.registryReads.get()).isEqualTo(3)

        // A full last page costs one more, empty, read.
        fake.reset()
        (250 until 300).forEach { fake.add("c%04d".format(it)) }
        assertThat(store.listCatalogs(null)).hasSize(300)
        assertThat(fake.registryReads.get()).isEqualTo(4)
    }

    @Test
    fun `a visibility-filtered list reads only the visible catalogs' tenants`() {
        val fake = FakeEngine()
        listOf("a", "b", "c").forEach { fake.add(it) }
        val store = store(fake)

        assertThat(store.listCatalogs(setOf("b", "zz")).map { it.name }).containsExactly("b")
        assertThat(fake.tenantsRead).containsExactly("t-b")
        assertThat(fake.requests.get()).isEqualTo(2)

        // Nothing visible: the registry read only.
        fake.reset()
        assertThat(store.listCatalogs(emptySet())).isEmpty()
        assertThat(fake.requests.get()).isEqualTo(1)
    }

    @Test
    fun `without the batch endpoint a list is one read per catalog plus one registry read, and re-asks later`() {
        val fake = FakeEngine().apply { batchSupported = false }
        val n = 40
        (0 until n).forEach { fake.add("c$it") }
        val store = store(fake)

        assertThat(store.listCatalogs(null)).hasSize(n)
        // 1 registry read + 1 batch attempt (404) + N meta reads, in
        // parallel; no per-catalog registry lookup, no migration probes.
        assertThat(fake.registryReads.get()).isEqualTo(1)
        assertThat(fake.tenantReads.get()).isEqualTo(n)
        assertThat(fake.requests.get()).isEqualTo(n + 2)

        // The engine's answer is remembered: no batch attempt per list.
        fake.reset()
        assertThat(store.listCatalogs(null)).hasSize(n)
        assertThat(fake.requests.get()).isEqualTo(n + 1)
    }

    @Test
    fun `a batch item that fails transiently is retried on its own`() {
        val fake = FakeEngine()
        listOf("a", "b", "c").forEach { fake.add(it) }
        fake.flakyInBatch += "t-b"
        val store = store(fake)

        assertThat(store.listCatalogs(null).map { it.name }).containsExactly("a", "b", "c")
        assertThat(fake.batches.get()).isEqualTo(1)
        assertThat(fake.tenantReads.get()).isEqualTo(1)
    }

    @Test
    fun `a whale-class catalog answered 421 in the batch is read from the whale pool`() {
        val fake = FakeEngine()
        listOf("a", "w").forEach { fake.add(it) }
        fake.whales += "t-w"
        // The whale pool: its own engine, holding only the whale tenant.
        val whale = FakeEngine().apply { tenants["t-w"] = Meta("w", "s3://b/w-in-whale-pool") }
        val store =
            HogliteCatalogStore(
                HogliteClient(standardUrl = serve(fake), whaleUrl = serve(whale), retryBudgetMs = 2_000, sleeper = {}),
            )

        val listed = store.listCatalogs(null)

        assertThat(
            listed.map { it.name to it.dataPath },
        ).containsExactly("a" to "s3://b/a", "w" to "s3://b/w-in-whale-pool")
        assertThat(fake.batches.get()).isEqualTo(1)
        // The retry's 421 pins w to the whale pool, which answers it.
        assertThat(whale.tenantReads.get()).isEqualTo(1)
    }

    // ---- incarnations --------------------------------------------------------

    @Test
    fun `a catalog dropped or recreated after the registry read is never listed from the wrong incarnation`() {
        val fake = FakeEngine()
        listOf("a", "b", "c").forEach { fake.add(it) }
        fake.add("e", tenant = "t-e")
        fake.tenants["t-e"] = Meta("not-e", "s3://b/elsewhere") // a tenant that is some other catalog's
        val store = store(fake)

        fake.afterRegistryRead = {
            fake.afterRegistryRead = {}
            // b: dropped (tenant deleted, registry row gone).
            fake.tenants.remove("t-b")
            fake.catalogs.remove("b")
            // c: dropped and recreated as a new incarnation.
            fake.tenants.remove("t-c")
            fake.add("c", tenant = "t-c-2", dataPath = "s3://b/c-2")
        }

        val listed = store.listCatalogs(null)

        assertThat(listed.map { it.name }).containsExactly("a")
        // The list read the incarnations its registry read named, and
        // never the one created after it.
        assertThat(fake.tenantsRead).containsExactlyInAnyOrder("t-a", "t-b", "t-c", "t-e")

        // The next list sees the new incarnation of c, and only it.
        val next = store.listCatalogs(null)
        assertThat(next.map { it.name to it.dataPath }).containsExactly("a" to "s3://b/a", "c" to "s3://b/c-2")
    }
}
