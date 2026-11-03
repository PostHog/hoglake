package com.posthog.hoglake.hoglite

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

/** Independent historical tenant + real upgrade path must equal a fresh tenant. */
@Tag("integration")
class HogliteSchemaEquivalenceIntegrationTest {
    @Test
    fun `folded tenant migrations match canonical DDL including columns constraints and indexes`() {
        HogliteConformanceEngine().use { engine ->
            val baseline =
                checkNotNull(javaClass.getResource("/hoglite/tenant-v0.sql")).readText()
                    .lineSequence().filterNot { it.trimStart().startsWith("--") }.joinToString("\n")
                    .split(';').map { it.trim() }.filter { it.isNotEmpty() }
            val migrated = create(engine, baseline)
            val canonical = create(engine, HogliteSchema.DDL)
            // The production first-operation upgrade, including its guards/probes.
            engine.store.ensureTenantSchema(migrated)
            assertThat(describe(engine, migrated)).isEqualTo(describe(engine, canonical))
            // A new process must be able to replay the upgrades without changing shape.
            HogliteCatalogStore(engine.client).ensureTenantSchema(migrated)
            assertThat(describe(engine, migrated)).isEqualTo(describe(engine, canonical))
        }
    }

    @Test
    fun `legacy data upgrade preserves every DV epoch and enforces one live row`() {
        HogliteConformanceEngine().use { engine ->
            // The frozen fixture supplies an independent historical schema.
            val old = historicalDdl()
            for (ddl in listOf(old, HogliteSchema.DDL.filterNot { "dvs_one_live_per_data_file" in it })) {
                val tenant = create(engine, ddl)
                seed(engine, tenant)
                val before = rows(engine, tenant)
                engine.store.ensureTenantSchema(tenant)
                assertThat(rows(engine, tenant)).isEqualTo(before)
                assertLiveIndex(engine, tenant)
                HogliteCatalogStore(engine.client).ensureTenantSchema(tenant)
                assertThat(rows(engine, tenant)).isEqualTo(before)
                assertLiveTransactions(engine, tenant)
            }
        }
    }

    @Test
    fun `conflicting legacy live rows refuse migration without changing data or caching success`() {
        HogliteConformanceEngine().use { engine ->
            val tenant = create(engine, historicalDdl())
            seed(engine, tenant)
            execute(engine, tenant, dv(1, 4, null))
            val before = rows(engine, tenant)
            for (store in listOf(engine.store, engine.store, HogliteCatalogStore(engine.client))) {
                val result = runCatching { store.ensureTenantSchema(tenant) }
                assertThat(result.exceptionOrNull()).isInstanceOf(HogliteRejected::class.java)
                assertThat(rows(engine, tenant)).isEqualTo(before)
                assertThat(
                    engine.client.query(
                        tenant,
                        "SELECT COUNT(*) FROM sqlite_master WHERE name = 'dvs_one_live_per_data_file'",
                    ).firstRow()!![0].asLong(),
                ).isZero()
            }
        }
    }

    @Test
    fun `fresh bootstrap enforces the independent live DV index shape and transaction order`() {
        HogliteConformanceEngine().use { engine ->
            val name = "dv" + UUID.randomUUID().toString().replace("-", "")
            engine.store.createCatalog(name, "s3://hoglake-it/$name")
            val tenant = engine.store.resolveTenant(name)
            assertLiveIndex(engine, tenant)
            seed(engine, tenant, meta = false)
            assertLiveTransactions(engine, tenant)
        }
    }

    @Test
    fun `two stores race the legacy live DV migration with an in-transaction probe`() {
        HogliteConformanceEngine().use { engine ->
            val tenant = create(engine, HogliteSchema.DDL.filterNot { "dvs_one_live_per_data_file" in it })
            seed(engine, tenant)
            val before = rows(engine, tenant)
            val probed = java.util.concurrent.CountDownLatch(2)
            val statuses = java.util.concurrent.CopyOnWriteArrayList<Int>()
            val sent = java.util.concurrent.atomic.AtomicInteger()
            val http = java.net.http.HttpClient.newHttpClient()
            val handlers = java.util.concurrent.Executors.newCachedThreadPool()
            val proxy = com.sun.net.httpserver.HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0)
            proxy.executor = handlers
            proxy.createContext("/") { ex ->
                try {
                    val path = ex.requestURI.toString()
                    val body = ex.requestBody.readBytes().decodeToString()
                    sent.incrementAndGet()
                    val req =
                        java.net.http.HttpRequest.newBuilder(java.net.URI.create(engine.url() + path))
                            .header("Content-Type", "application/json")
                            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build()
                    val response = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofByteArray())
                    if (path.endsWith("/query") && "SELECT (" in body && probed.count > 0) {
                        probed.countDown()
                        check(probed.await(10, java.util.concurrent.TimeUnit.SECONDS))
                    }
                    if (path.endsWith("/tx") && "CREATE UNIQUE INDEX" in body) statuses += response.statusCode()
                    ex.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                    ex.responseBody.use { it.write(response.body()) }
                } catch (_: Throwable) {
                    ex.sendResponseHeaders(599, -1)
                    ex.close()
                }
            }
            proxy.start()
            val pool = java.util.concurrent.Executors.newFixedThreadPool(2)
            val url = "http://127.0.0.1:${proxy.address.port}"
            try {
                HogliteClient(standardUrl = url).use { a ->
                    HogliteClient(standardUrl = url).use { b ->
                        val first = HogliteCatalogStore(a)
                        val second = HogliteCatalogStore(b)
                        val jobs =
                            listOf(first, second).map {
                                    store ->
                                pool.submit { store.ensureTenantSchema(tenant) }
                            }
                        jobs.forEach { it.get(20, java.util.concurrent.TimeUnit.SECONDS) }
                        assertThat(probed.count).isZero()
                        assertThat(statuses.sorted()).containsExactly(200, 409)
                        assertLiveIndex(engine, tenant)
                        assertThat(rows(engine, tenant)).isEqualTo(before)
                        val warm = sent.get()
                        first.ensureTenantSchema(tenant)
                        second.ensureTenantSchema(tenant)
                        assertThat(sent.get()).isEqualTo(warm)
                        HogliteCatalogStore(a).ensureTenantSchema(tenant)
                        assertThat(sent.get()).isEqualTo(warm + 1)
                        assertThat(statuses).hasSize(2)
                    }
                }
            } finally {
                pool.shutdownNow()
                proxy.stop(0)
                handlers.shutdownNow()
                http.close()
            }
        }
    }

    private fun historicalDdl(): List<String> =
        checkNotNull(javaClass.getResource("/hoglite/tenant-v0.sql")).readText()
            .lineSequence().filterNot { it.trimStart().startsWith("--") }.joinToString("\n")
            .split(';').map { it.trim() }.filter { it.isNotEmpty() }

    private fun seed(
        engine: HogliteConformanceEngine,
        tenant: String,
        meta: Boolean = true,
    ) {
        val statements = mutableListOf<String>()
        if (meta) statements += "INSERT INTO meta(id, name, data_path) VALUES(1, 'legacy', 's3://hoglake-it/legacy')"
        statements += "INSERT INTO namespaces(id, name, created_snapshot) VALUES(1, 'ns', 0)"
        statements += "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, created_snapshot) " +
            "VALUES(1, 1, 'ns', 't', '00000000-0000-0000-0000-000000000001', 0)"
        for (id in 1..3) {
            statements += "INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start, " +
                "stats_state, begin_snapshot) VALUES($id, 1, 'f$id', 10, 100, ${id * 10}, 'ready', 0)"
        }
        statements += listOf(dv(1, 1, 2), dv(1, 2, 3), dv(1, 3, null), dv(2, 1, null))
        execute(engine, tenant, *statements.toTypedArray())
    }

    private fun dv(
        file: Int,
        epoch: Int,
        end: Int?,
        table: Int = 1,
    ): String =
        "INSERT INTO dvs(data_file_id, table_id, delete_file_id, path, delete_count, file_size_bytes, " +
            "begin_snapshot, end_snapshot, epoch) VALUES($file, $table, ${file * 100 + epoch}, " +
            "'dv-$file-$epoch', $epoch, ${100 + epoch}, $epoch, ${end ?: "NULL"}, $epoch)"

    private fun execute(
        engine: HogliteConformanceEngine,
        tenant: String,
        vararg sql: String,
    ) = engine.client.tx(tenant, UUID.randomUUID().toString(), null, sql.map { HogliteStatement(it) })

    private fun rows(
        engine: HogliteConformanceEngine,
        tenant: String,
    ) = engine.client.query(tenant, "SELECT * FROM dvs ORDER BY data_file_id, epoch").rows

    private fun assertLiveIndex(
        engine: HogliteConformanceEngine,
        tenant: String,
    ) {
        // This oracle does not reference the production DDL constant.
        val definitions =
            engine.client.query(
                tenant,
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'dvs' AND sql IS NOT NULL",
            ).rows.map { it[0].asString().lowercase().replace(Regex("\\s+"), " ").trim() }
        val expected =
            Regex(
                "create unique index(?: if not exists)? dvs_one_live_per_data_file on dvs\\s*" +
                    "\\(\\s*data_file_id\\s*\\) where end_snapshot is null",
            )
        assertThat(definitions.filter { expected.matches(it) }).hasSize(1)
    }

    private fun assertLiveTransactions(
        engine: HogliteConformanceEngine,
        tenant: String,
    ) {
        val before = rows(engine, tenant)
        // A different table_id cannot weaken a data_file_id invariant.
        assertThatThrownBy {
            execute(engine, tenant, "UPDATE meta SET consumer_floor = 0 WHERE id = 1", dv(1, 4, null, table = 2))
        }.isInstanceOf(HogliteRejected::class.java)
        assertThat(rows(engine, tenant)).isEqualTo(before)
        assertThat(engine.client.query(tenant, "SELECT consumer_floor FROM meta").firstRow()!![0].asLong()).isEqualTo(1)
        assertThatThrownBy {
            execute(
                engine,
                tenant,
                dv(1, 4, null),
                "UPDATE dvs SET end_snapshot = 4 WHERE data_file_id = 1 AND epoch = 3",
            )
        }.isInstanceOf(HogliteRejected::class.java)
        assertThat(rows(engine, tenant)).isEqualTo(before)
        execute(engine, tenant, "UPDATE dvs SET end_snapshot = 4 WHERE data_file_id = 1 AND epoch = 3", dv(1, 4, null))
        assertThat(
            engine.client.query(
                tenant,
                "SELECT epoch FROM dvs WHERE data_file_id = 1 AND begin_snapshot <= 3 " +
                    "AND (end_snapshot IS NULL OR end_snapshot > 3)",
            ).rows.map { it[0].asLong() },
        ).containsExactly(3L)
        assertThat(
            engine.client.query(tenant, "SELECT COUNT(*) FROM dvs WHERE data_file_id = 1").firstRow()!![0].asLong(),
        )
            .isEqualTo(4)
    }

    private fun create(
        engine: HogliteConformanceEngine,
        ddl: List<String>,
    ): String {
        val tenant = "schema-" + UUID.randomUUID()
        engine.client.createTenant(tenant)
        engine.client.tx(tenant, UUID.randomUUID().toString(), null, ddl.map { HogliteStatement(it) })
        return tenant
    }

    /** SQLite's own stored definitions include columns/defaults/checks/FKs and index predicates. */
    private fun describe(
        engine: HogliteConformanceEngine,
        tenant: String,
    ): List<String> =
        engine.client.query(
            tenant,
            "SELECT type, name, tbl_name, sql FROM sqlite_master " +
                "WHERE name NOT LIKE 'sqlite_%' AND name NOT LIKE '_hw_%' ORDER BY type, name",
        ).rows.map { row ->
            row.joinToString("|") { value ->
                value?.toString()?.replace(Regex("/\\*.*?\\*/", RegexOption.DOT_MATCHES_ALL), "")
                    ?.replace(Regex("(?i)IF NOT EXISTS\\s+"), "")
                    ?.replace(Regex("\\s+"), " ")
                    ?.replace(Regex("\\s*([(),])\\s*"), "$1")?.trim() ?: ""
            }
        }.sorted()
}
