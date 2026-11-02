package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * The registry tenant's bootstrap, against a scripted hoglite: it runs
 * once per process (not once per create or per background list), it
 * reads before it creates, a registry reported missing is re-created by
 * the next write only, a 421 on a registry call is retried once against
 * the owner it names, and a registry that cannot be reached fails with
 * the step and the underlying cause.
 *
 * hoglite is faked, so this runs in the unit phase. The same contract
 * against the real engine is in HogliteBackendIntegrationTest
 * (`the registry is bootstrapped once per process ...`).
 */
class HogliteRegistryBootstrapTest {
    private val servers = mutableListOf<HttpServer>()

    @AfterEach
    fun stop() = servers.forEach { it.stop(0) }

    private val json = jacksonObjectMapper()

    private val registry = "hoglake-__hoglake_registry"

    /**
     * The registry tenant as hoglite serves it: `PUT /v1/tenants/{t}`,
     * `/query`, `/tx`, with the engine's statuses for a missing tenant
     * (404) and a missing table (409 on /query, 422 on /tx, both
     * `refused`). Counts what the bootstrap sends.
     */
    private inner class FakeRegistry {
        @Volatile var tenantExists = true

        @Volatile var tableExists = true

        @Volatile var ddl = "CREATE TABLE catalogs(name TEXT PRIMARY KEY, data_path TEXT NOT NULL, tenant TEXT)"
        val names = ConcurrentLinkedQueue<String>()
        val puts = AtomicInteger()
        val schemaReads = AtomicInteger()
        val bootTxs = AtomicInteger()
        val requests = AtomicInteger()
        val txBodies = ConcurrentLinkedQueue<String>()

        fun handle(ex: HttpExchange) {
            requests.incrementAndGet()
            val path = ex.requestURI.path
            val body = ex.requestBody.readBytes().decodeToString()
            when {
                ex.requestMethod == "PUT" && path == "/v1/tenants/$registry" -> {
                    puts.incrementAndGet()
                    tenantExists = true
                    reply(ex, 200, """{"tenant":"$registry","owner":"hl-0"}""")
                }
                path == "/v1/t/$registry/query" -> query(ex, json.readValue<Map<String, Any?>>(body)["sql"] as String)
                path == "/v1/t/$registry/tx" -> tx(ex, body)
                else -> reply(ex, 500, """{"error":"unexpected $path"}""")
            }
        }

        private fun query(
            ex: HttpExchange,
            sql: String,
        ) {
            if ("sqlite_master" in sql) schemaReads.incrementAndGet()
            if (!tenantExists) return reply(ex, 404, """{"error":"tenant does not exist"}""")
            if ("sqlite_master" in sql) {
                val rows = if (tableExists) listOf(listOf(ddl)) else emptyList()
                return reply(ex, 200, json.writeValueAsString(mapOf("cols" to listOf("sql"), "rows" to rows)))
            }
            if (!tableExists) {
                return reply(ex, 409, """{"error":"SQL logic error: no such table: catalogs","code":"refused"}""")
            }
            val rows = names.sorted().map { listOf(it) }
            reply(ex, 200, json.writeValueAsString(mapOf("cols" to listOf("name"), "rows" to rows)))
        }

        private fun tx(
            ex: HttpExchange,
            body: String,
        ) {
            if (!tenantExists) return reply(ex, 404, """{"error":"tenant does not exist"}""")
            txBodies += body

            @Suppress("UNCHECKED_CAST")
            val statements = json.readValue<Map<String, Any?>>(body)["statements"] as List<Map<String, Any?>>
            val first = statements.first()["sql"] as String
            if (first.startsWith("CREATE TABLE")) {
                bootTxs.incrementAndGet()
                tableExists = true
            } else if (!tableExists) {
                return reply(ex, 422, """{"error":"SQL logic error: no such table: catalogs","code":"refused"}""")
            }
            val results = statements.map { mapOf("cols" to listOf("n"), "rows" to listOf(listOf(0)), "affected" to 1) }
            reply(ex, 200, json.writeValueAsString(mapOf("results" to results)))
        }
    }

    private fun reply(
        ex: HttpExchange,
        status: Int,
        body: String,
    ) {
        val bytes = body.toByteArray()
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    private fun serve(handler: (HttpExchange) -> Unit): HttpServer {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newFixedThreadPool(8)
        server.createContext("/") { ex -> handler(ex) }
        server.start()
        servers += server
        return server
    }

    private fun url(server: HttpServer) = "http://127.0.0.1:${server.address.port}"

    private fun store(
        url: String,
        ownerUrlPattern: String = "",
        readTimeoutMs: Long = 5_000,
        budgetMs: Long = 2_000,
    ) = HogliteCatalogStore(
        HogliteClient(
            standardUrl = url,
            ownerUrlPattern = ownerUrlPattern,
            readTimeoutMs = readTimeoutMs,
            retryBudgetMs = budgetMs,
            sleeper = {},
        ),
    )

    private fun registryRow(name: String) =
        HogliteTx().apply {
            expectZero("SELECT COUNT(*) FROM catalogs WHERE name = ?", name) {
                HoglakeException.AlreadyExists("catalog '$name' already exists")
            }
            add("INSERT INTO catalogs(name, data_path, tenant) VALUES (?, ?, ?)", name, "s3://b/$name", "t-$name")
        }

    // ---- once, not per operation -------------------------------------------

    @Test
    fun `an existing registry is checked once, with one read and no create, across N operations`() {
        val fake = FakeRegistry()
        fake.names += "a"
        val store = store(url(serve(fake::handle)))
        // N concurrent first callers: one bootstrap, the rest wait for it.
        val n = 8
        val pool = Executors.newFixedThreadPool(n)
        val go = CountDownLatch(1)
        val done = (1..n).map { pool.submit { go.await().also { store.ensureRegistry() } } }
        go.countDown()
        done.forEach { it.get(10, TimeUnit.SECONDS) }
        pool.shutdown()
        // Then N of every registry call a create or a background loop makes.
        repeat(n) {
            store.ensureRegistry()
            assertThat(store.listCatalogNames()).containsExactly("a")
            store.runRegistryTx(HogliteCatalogStore.freshTxid("create-catalog"), null, registryRow("c$it"))
        }
        fake.names.clear()
        repeat(n) { assertThat(store.listCatalogs(null)).isEmpty() }

        assertThat(fake.puts.get()).describedAs("registry PUTs").isZero()
        assertThat(fake.bootTxs.get()).describedAs("registry bootstrap transactions").isZero()
        assertThat(fake.schemaReads.get()).describedAs("registry schema reads").isEqualTo(1)
        assertThat(store.registryReadyForTests).isTrue()
    }

    @Test
    fun `a fresh deployment creates the registry exactly once`() {
        val fake =
            FakeRegistry().apply {
                tenantExists = false
                tableExists = false
            }
        val store = store(url(serve(fake::handle)))
        repeat(5) {
            store.ensureRegistry()
            store.runRegistryTx(HogliteCatalogStore.freshTxid("create-catalog"), null, registryRow("c$it"))
            store.listCatalogNames()
        }
        assertThat(fake.puts.get()).isEqualTo(1)
        assertThat(fake.bootTxs.get()).isEqualTo(1)
        assertThat(fake.schemaReads.get()).isEqualTo(1)
    }

    @Test
    fun `a registry from before incarnation ids gets its tenant column once`() {
        val fake = FakeRegistry()
        fake.ddl = "CREATE TABLE catalogs(name TEXT PRIMARY KEY, data_path TEXT NOT NULL)"
        val store = store(url(serve(fake::handle)))
        repeat(3) { store.ensureRegistry() }
        assertThat(fake.txBodies.filter { "ALTER TABLE catalogs ADD COLUMN tenant" in it }).hasSize(1)
        assertThat(fake.puts.get()).isZero()
    }

    // ---- background lists only read ----------------------------------------

    @Test
    fun `the background loops' list never creates the registry`() {
        val fake =
            FakeRegistry().apply {
                tenantExists = false
                tableExists = false
            }
        val store = store(url(serve(fake::handle)))
        repeat(5) { assertThat(store.listCatalogNames()).isEmpty() }
        assertThat(store.listCatalogs(null)).isEmpty()
        // A tenant with no table yet (a create that died between the two) reads as empty too.
        fake.tenantExists = true
        assertThat(store.listCatalogNames()).isEmpty()
        assertThat(fake.puts.get()).isZero()
        assertThat(fake.bootTxs.get()).isZero()
        assertThat(fake.schemaReads.get()).describedAs("bootstrap reads").isZero()
        assertThat(store.registryReadyForTests).isFalse()
    }

    @Test
    fun `a 404 that is not hoglite's unknown-tenant answer is an error, not an empty registry`() {
        // A wrong HOGLAKE_HOGLITE_URL, or a proxy in front that does not know the route.
        val wrong =
            serve { ex ->
                ex.requestBody.readBytes()
                reply(ex, 404, "404 page not found")
            }
        val store = store(url(wrong))
        val e = catchThrowable { store.listCatalogNames() }
        assertThat(e).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(e.message)
            .contains("catalog registry (hoglite tenant '$registry') is unavailable: reading the registry failed")
            .contains("hoglite 404: 404 page not found")
        val e2 = catchThrowable { store.ensureRegistry() }
        assertThat(e2).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(e2.message).contains("reading the registry schema failed: hoglite 404")
    }

    // ---- invalidation -------------------------------------------------------

    @Test
    fun `a registry reported missing is re-created by the next write, and only then`() {
        val fake = FakeRegistry()
        val store = store(url(serve(fake::handle)))
        store.ensureRegistry()
        assertThat(store.registryReadyForTests).isTrue()

        // The registry tenant goes away under a running process.
        fake.tenantExists = false
        fake.tableExists = false
        // A read notices, clears the cache, answers empty and creates nothing.
        assertThat(store.listCatalogNames()).isEmpty()
        assertThat(store.registryReadyForTests).isFalse()
        assertThat(fake.puts.get()).isZero()

        // A write sent while the cache still said "ready" (another thread's
        // create): 404, bootstrap, the SAME transaction once more.
        store.ensureRegistry() // re-reads, finds it missing, creates it
        assertThat(fake.puts.get()).isEqualTo(1)
        fake.tenantExists = false
        fake.tableExists = false
        fake.txBodies.clear()
        val txid = HogliteCatalogStore.freshTxid("create-catalog")
        store.runRegistryTx(txid, null, registryRow("x"))
        assertThat(fake.puts.get()).isEqualTo(2)
        assertThat(fake.bootTxs.get()).isEqualTo(2)
        // The row went once (the 404 was before the fake recorded it), with the caller's txid.
        val rowTxs = fake.txBodies.filter { "INSERT INTO catalogs" in it }
        assertThat(rowTxs).hasSize(1)
        assertThat(rowTxs.single()).contains(txid)
        assertThat(store.registryReadyForTests).isTrue()

        // A missing TABLE (tenant present) is the same signal, from /tx's 422.
        fake.tableExists = false
        store.runRegistryTx(HogliteCatalogStore.freshTxid("create-catalog"), null, registryRow("y"))
        assertThat(fake.bootTxs.get()).isEqualTo(3)
        assertThat(fake.puts.get()).isEqualTo(2)
    }

    // ---- 421: one retry against the owner ------------------------------------

    @Test
    fun `a 421 on a registry call is retried once, against the owner it names`() {
        val fake = FakeRegistry()
        val owner = serve(fake::handle)
        val entryCalls = AtomicInteger()
        val entry =
            serve { ex ->
                entryCalls.incrementAndGet()
                ex.requestBody.readBytes()
                reply(ex, 421, """{"error":"not owner","owner":"${owner.address.port}"}""")
            }
        val store = store(url(entry), ownerUrlPattern = "http://127.0.0.1:{owner}")

        // The bootstrap read: one 421 at the service, one retry at the owner.
        store.ensureRegistry()
        assertThat(entryCalls.get()).isEqualTo(1)
        assertThat(fake.requests.get()).isEqualTo(1)
        // The pin holds: later registry calls go to the owner directly.
        store.listCatalogNames()
        store.runRegistryTx(HogliteCatalogStore.freshTxid("create-catalog"), null, registryRow("z"))
        assertThat(entryCalls.get()).isEqualTo(1)
        assertThat(fake.requests.get()).isEqualTo(3)
    }

    // ---- an honest failure ----------------------------------------------------

    @Test
    fun `an engine that does not answer fails the bootstrap with the step and the timeouts, within the budget`() {
        val stall =
            serve { ex ->
                ex.requestBody.readBytes()
                Thread.sleep(1_500)
                reply(ex, 200, "{}")
            }
        val store = store(url(stall), readTimeoutMs = 100, budgetMs = 450)
        val started = System.nanoTime()
        val e = catchThrowable { store.ensureRegistry() }
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        assertThat(e).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(e).isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
        assertThat(e.message)
            .contains("catalog registry (hoglite tenant '$registry') is unavailable")
            .contains("reading the registry schema failed")
            .contains("POST /v1/t/$registry/query")
            .containsPattern("after [0-9]+ attempts: [0-9]+ timed out \\(attempt timeout up to [0-9]+ms\\)")
        assertThat(e.cause).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(elapsedMs).isLessThan(1_500)
        assertThat(store.registryReadyForTests).isFalse()
    }

    @Test
    fun `a routing failure says it was misdirected and to which owner`() {
        val entry =
            serve { ex ->
                ex.requestBody.readBytes()
                reply(ex, 421, """{"error":"not owner","owner":"hl-2.4f1c"}""")
            }
        // No owner-direct routing: the service is the only address there is.
        val e = catchThrowable { store(url(entry), budgetMs = 300).listCatalogNames() }
        assertThat(e).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(e.message)
            .contains("catalog registry (hoglite tenant '$registry') is unavailable: reading the registry failed")
            .containsPattern("[0-9]+ misdirected \\(421\\)")
            .contains("last 421 named owner 'hl-2'")
            .contains("owner-direct routing is not configured (HOGLAKE_HOGLITE_OWNER_URL_PATTERN)")
    }

    @Test
    fun `a refused create and a closed port each surface their own cause`() {
        val fake = FakeRegistry().apply { tenantExists = false }
        val refusing =
            serve { ex ->
                if (ex.requestMethod == "PUT") {
                    ex.requestBody.readBytes()
                    reply(ex, 500, """{"error":"manifest put: AccessDenied"}""")
                } else {
                    fake.handle(ex)
                }
            }
        val e = catchThrowable { store(url(refusing)).ensureRegistry() }
        assertThat(e).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(e.message)
            .contains("creating the registry tenant failed")
            .contains("hoglite 500")
            .contains("manifest put: AccessDenied")

        val closed = ServerSocket(0).use { it.localPort }
        val e2 = catchThrowable { store("http://127.0.0.1:$closed", budgetMs = 300).ensureRegistry() }
        assertThat(e2).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(e2.message)
            .contains("reading the registry schema failed")
            .containsPattern("[0-9]+ connect refused")
    }

    @Test
    fun `startup bootstrap is best effort and a later create retries it`() {
        val fake = FakeRegistry()
        val down = AtomicInteger(1)
        val flaky =
            serve { ex ->
                if (down.get() == 1) {
                    ex.requestBody.readBytes()
                    reply(ex, 503, """{"error":"draining","code":"not_applied"}""")
                } else {
                    fake.handle(ex)
                }
            }
        val store = store(url(flaky), budgetMs = 200)
        assertThat(store.bootstrapRegistryAtStartup()).isFalse()
        assertThat(store.registryReadyForTests).isFalse()
        down.set(0)
        store.ensureRegistry()
        assertThat(store.registryReadyForTests).isTrue()
        assertThat(fake.schemaReads.get()).isEqualTo(1)
    }
}
