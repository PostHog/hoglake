package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * A remote schema end to end on its Host (docs/auth-tenancy.md §6.5d): the
 * real application over PostgreSQL, serving the namespace `posthog` of the
 * source catalog to readers that arrive as a Client relays them, on
 * `/v1/catalogs/<orgID>/namespaces/posthog/...` with the caller's own token.
 *
 * The source catalog holds teams of two orgs in the same tables, a second
 * namespace with a table of its own, and tables the schema cannot scope.
 * Each test checks what the routes answer and, for a refusal, that nothing
 * was read: no record looked up and no statement sent.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServedSchemaIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-served-schema-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val source = "millrace-dev"

    /** The reader's org: its record holds teams 7, 42 and 49. */
    private val org = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0"

    /** Another org, in the same tables: teams 99 and 500. */
    private val otherOrg = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1"

    /** An org with no record at all, and one whose record is disabled. */
    private val noRecordOrg = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f2"
    private val disabledOrg = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f3"

    private val schema = "/v1/catalogs/$org/namespaces/posthog"
    private val four = listOf(schema, "$schema/tables", "$schema/tables/events", "$schema/tables/events/scan")

    /** Every record name the server asked the registry for. */
    private val lookups = CopyOnWriteArrayList<String>()
    private val registry =
        MountRegistry { name ->
            lookups += name
            when (name) {
                "$org-events" -> MountRecord(name, revision = 1, enabled = true, values = listOf(7, 42, 49))
                "$otherOrg-events" -> MountRecord(name, revision = 1, enabled = true, values = listOf(99, 500))
                "$disabledOrg-events" -> MountRecord(name, revision = 2, enabled = false, values = listOf(7, 42, 49))
                else -> null
            }
        }

    /** Statements sent on the test database while [counting] is on. */
    private val statements = AtomicInteger()

    @Volatile
    private var counting = false

    /** Every statement's text sent while [counting] is on. */
    private val sqls = CopyOnWriteArrayList<String>()

    /** The app of the Host [withHost] is running, for tests that hold its scan cap. */
    private lateinit var currentApp: App

    private val events: TableInfo
    private val teamFieldId: Long
    private val sourceCatalogId: Long

    init {
        catalogs.createCatalog(source, "s3://bucket/millrace/$source")
        catalogs.createNamespace(source, "posthog")
        catalogs.createNamespace(source, "ingest")
        events = catalogs.createTable(source, "posthog", "events", listOf(ColumnDef("team_id", ColType.LONG)))
        val persons = catalogs.createTable(source, "posthog", "persons", listOf(ColumnDef("team_id", ColType.LONG)))
        // Two tables the schema cannot scope: no object-prefix field, and a prefix on another column.
        val plain = catalogs.createTable(source, "posthog", "no_layout", listOf(ColumnDef("team_id", ColType.LONG)))
        val byDay =
            catalogs.createTable(
                source,
                "posthog",
                "by_day",
                listOf(ColumnDef("day", ColType.LONG), ColumnDef("team_id", ColType.LONG)),
            )
        // A table of another namespace of the source catalog, laid out like a served one.
        val secret = catalogs.createTable(source, "ingest", "secret_events", listOf(ColumnDef("team_id", ColType.LONG)))
        // A table whose layout names the team field and whose specs do not: it cannot be scoped.
        val broken = catalogs.createTable(source, "posthog", "broken", listOf(ColumnDef("team_id", ColType.LONG)))
        teamFieldId = events.columns.single { it.def.name == "team_id" }.fieldId
        sourceCatalogId =
            db.jdbi.withHandle<Long, Exception> { h ->
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n").bind("n", source)
                    .mapTo(Long::class.java).one()
            }
        db.jdbi.useHandle<Exception> { h ->
            val catId = sourceCatalogId
            h.execute(
                "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                    "VALUES (?, ?, 1, ?)",
                catId,
                broken.tableId,
                broken.columns.single().fieldId,
            )
            layout(h, catId, events, "team_id")
            layout(h, catId, persons, "team_id")
            layout(h, catId, byDay, "day")
            layout(h, catId, secret, "team_id")
            partitioned(h, catId, plain, "team_id")

            file(h, catId, events, 1, "42")
            file(h, catId, events, 2, "42")
            file(h, catId, events, 3, "7")
            file(h, catId, events, 4, "99") // another org's team
            file(h, catId, events, 5, "42", state = "pending") // stats not proven
            file(h, catId, events, 6, "42", quarantined = true) // footer did not prove the team
            file(h, catId, persons, 7, "7")
            file(h, catId, persons, 8, "500") // another org's team
            file(h, catId, plain, 9, "7")
            file(h, catId, byDay, 10, "7")
            file(h, catId, secret, 11, "7")
            h.execute(
                "INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, " +
                    "begin_snapshot, end_snapshot, path, file_format, delete_count, file_size_bytes) " +
                    "VALUES (?, 9001, ?, 1, 1, NULL, ?, 'puffin-dv', 3, 128)",
                catId,
                events.tableId,
                "s3://bucket/millrace/$source/data/posthog/events/team_id=42/dv1.puffin",
            )
            // Column statistics for this org's files and for the other org's file 4, and row groups for file 1.
            for (id in listOf(1L, 2L, 3L, 4L)) {
                h.execute(
                    "INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count, " +
                        "null_count, lower_bound, upper_bound) VALUES (?, ?, ?, 10, 0, " +
                        "'\\x0000000000000001'::bytea, '\\x0000000000000002'::bytea)",
                    catId,
                    id,
                    teamFieldId,
                )
            }
            h.execute("UPDATE hog_data_file SET split_offsets = ARRAY[4, 100]::bigint[] WHERE data_file_id = 1")
            // The real records, for the tests that run without the stub registry.
            for ((name, enabled, values) in listOf(
                Triple("$org-events", true, "{7,42,49}"),
                Triple("$otherOrg-events", true, "{99,500}"),
                Triple("$disabledOrg-events", false, "{7,42,49}"),
            )) {
                h.execute(
                    "INSERT INTO hog_mount (name, revision, enabled, mount_values) VALUES (?, 1, ?, ?::bigint[])",
                    name,
                    enabled,
                    values,
                )
            }
            h.execute("UPDATE hog_mount_switch SET enabled = true")
        }
        db.jdbi.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (counting) {
                        statements.incrementAndGet()
                        sqls += context.rawSql
                    }
                }
            },
        )
    }

    private fun partitioned(
        h: Handle,
        catId: Long,
        table: TableInfo,
        column: String,
    ): Long {
        val fieldId = table.columns.single { it.def.name == column }.fieldId
        h.execute(
            "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) VALUES (?, ?, 1, 0)",
            catId,
            table.tableId,
        )
        h.execute(
            "INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, " +
                "transform) VALUES (?, ?, 1, 0, ?, 'identity')",
            catId,
            table.tableId,
            fieldId,
        )
        return fieldId
    }

    /** [table] partitioned by [column], with that column as its object-prefix field. */
    private fun layout(
        h: Handle,
        catId: Long,
        table: TableInfo,
        column: String,
    ) {
        val fieldId = partitioned(h, catId, table, column)
        h.execute(
            "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                "VALUES (?, ?, 1, ?)",
            catId,
            table.tableId,
            fieldId,
        )
    }

    private fun file(
        h: Handle,
        catId: Long,
        table: TableInfo,
        id: Long,
        team: String,
        state: String = "provided",
        quarantined: Boolean = false,
    ) {
        h.execute(
            "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, " +
                "end_snapshot, path, record_count, file_size_bytes, row_id_start, spec_id, " +
                "stats_state, prefix_quarantined) VALUES (?, ?, ?, 1, NULL, ?, ?, ?, ?, 1, ?, ?)",
            catId,
            id,
            table.tableId,
            "s3://bucket/millrace/$source/data/${table.name}/team_id=$team/f$id.parquet",
            id * 100,
            id * 10,
            id * 1000,
            state,
            quarantined,
        )
        h.execute(
            "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) VALUES (?, ?, 0, ?)",
            catId,
            id,
            team,
        )
    }

    @AfterAll
    fun close() {
        hosts.values.forEach { it.close() }
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun servedFile(): String =
        Files.createTempFile("hoglake-served-schemas-", ".json").also {
            it.toFile().deleteOnExit()
            Files.writeString(it, """{"schemas":[{"name":"posthog","catalog":"$source"}]}""")
        }.toString()

    /** The Host. [served] false is the same server without the setting. */
    private fun <T> withHost(
        served: Boolean = true,
        authMode: String = "enforce",
        fileBudget: Int = 2_000_000,
        deadlineMs: Long = 60_000,
        maxConcurrent: Int = 8,
        /** Null: the stub registry. A set: the real `MountStore` over `hog_mount`, with this allowlist. */
        allowlist: Set<String>? = null,
        postgresBackendId: String = "",
        /** Serve as the public listener does: always `enforce`, operator tokens dropped. */
        publicListener: Boolean = false,
        /**
         * A Host of its own, stopped when [block] returns: for a test that reads `/metrics`, which
         * reports the app built last. Otherwise one Host per configuration serves the whole class.
         */
        fresh: Boolean = false,
        block: (port: Int) -> T,
    ): T {
        val key =
            listOf(
                served,
                authMode,
                fileBudget,
                deadlineMs,
                maxConcurrent,
                allowlist,
                postgresBackendId,
                publicListener,
            )
        if (!fresh) {
            val host = hosts.getOrPut(key) { startHost(key) }
            currentApp = host.app
            return block(host.port)
        }
        val host = startHost(key)
        try {
            currentApp = host.app
            return block(host.port)
        } finally {
            host.close()
        }
    }

    private class RunningHost(
        val app: App,
        val port: Int,
        private val stop: () -> Unit,
    ) : AutoCloseable {
        override fun close() {
            stop()
            app.close()
        }
    }

    /** The Hosts this class keeps running, by configuration. */
    private val hosts = LinkedHashMap<List<Any?>, RunningHost>()

    @Suppress("UNCHECKED_CAST")
    private fun startHost(key: List<Any?>): RunningHost {
        val served = key[0] as Boolean
        val authMode = key[1] as String
        val fileBudget = key[2] as Int
        val deadlineMs = key[3] as Long
        val maxConcurrent = key[4] as Int
        val allowlist = key[5] as Set<String>?
        val postgresBackendId = key[6] as String
        val publicListener = key[7] as Boolean
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                servedSchemasFile = if (served) servedFile() else "",
                // The legacy mount, still configured beside it: one pinned table.
                mountSourceCatalog = source,
                mountSourceTableUuid = events.tableUuid.toString(),
                mountSourceFieldId = teamFieldId.toString(),
                mountScanFileBudget = fileBudget,
                mountScanDeadlineMs = deadlineMs,
                mountScanMaxConcurrent = maxConcurrent,
                mountAllowlist = allowlist ?: emptySet(),
                postgresBackendId = postgresBackendId,
                authMode = authMode,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = if (authMode == "off") "" else keyFile.toString(),
                authPredicateFields = "team_id",
                authSessionProfile = "on",
            )
        val app = App.build(cfg, db.jdbi, mountRegistry = if (allowlist == null) registry else null)
        val server =
            embeddedServer(Netty, configure = { configureApiListener(cfg) }) {
                if (publicListener) app.module(this, app.auth.forPublicListener()) else app.module(this)
            }
        server.start(wait = false)
        return RunningHost(
            app,
            runBlocking { server.engine.resolvedConnectors().first().port },
        ) { server.stop(0, 1000) }
    }

    /**
     * A session token as a Client relays it: scoped, for [tokenOrg], with `read` on the catalog [cat]
     * and [schemas] as its `remote_schemas` (null leaves the claim out).
     */
    private fun reader(
        tokenOrg: String = org,
        cat: String = tokenOrg,
        schemas: String? = """{"posthog":["read"]}""",
        prd: String = """{"team_id":{"7":"read","42":"read"}}""",
        extra: String = "",
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                prd = prd,
                catalogs = """{"$cat":["read"]}""",
                iat = iat,
                org = tokenOrg,
                extra = TestTokens.sessionProfile(cat, schemas) + extra,
            ),
        )
    }

    /** A scoped token for the org's catalog with no session profile, so no `remote_schemas`. */
    private fun plainScoped(): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                prd = """{"team_id":{"*":"read"}}""",
                catalogs = """{"$org":["read"]}""",
                iat = iat,
                org = org,
            ),
        )
    }

    /** A tenant of the org's catalog that carries the claim: a tenant token may, and is still not served. */
    private fun tenant(): String =
        TestTokens.mint(
            pair,
            TestTokens.tenantClaims(org = org, catalogs = mapOf(org to listOf("admin"))).dropLast(1) +
                TestTokens.sessionProfile(org, """{"posthog":["read"]}""") + "}",
        )

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    /** A reader of the org's legacy mount, `<orgID>-events`. */
    private fun mountReader(): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                prd = """{"team_id":{"7":"read","42":"read"}}""",
                catalogs = """{"$org-events":["read"]}""",
                iat = iat,
                org = org,
            ),
        )
    }

    /** Every caller the rule refuses, by the condition it fails. */
    private fun refused(): Map<String, String> =
        linkedMapOf(
            "not scoped: an operator" to operator(),
            "not scoped: a tenant carrying the claim" to tenant(),
            "no claim: a plain scoped token" to plainScoped(),
            "no claim: a session token without remote_schemas" to reader(schemas = null),
            "a claim for another schema" to reader(schemas = """{"imports":["read"]}"""),
            "cat names another catalog" to reader(tokenOrg = org, cat = otherOrg),
            "org is not the path's catalog" to reader(tokenOrg = otherOrg, cat = org),
        )

    private fun send(
        port: Int,
        method: String,
        path: String,
        token: String?,
    ): HttpResponse<String> {
        val body = if (method == "GET" || method == "DELETE") null else "{}"
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
        token?.let { request.header("Authorization", "Bearer $it") }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString())
    }

    private fun get(
        port: Int,
        path: String,
        token: String?,
    ) = send(port, "GET", path, token)

    private fun json(response: HttpResponse<String>): JsonNode {
        assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(200)
        return mapper.readTree(response.body())
    }

    private fun code(response: HttpResponse<String>): String =
        response.body().substringAfter("\"error\":\"", "").substringBefore('"')

    /** The one refusal: the 404 of a missing namespace, in words that depend on the path alone. */
    private fun missing(catalog: String = org) =
        """{"error":"not_found","detail":"namespace 'posthog' in catalog '$catalog'"}"""

    private fun assertMissing(
        response: HttpResponse<String>,
        what: String,
        catalog: String = org,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(404)
        assertThat(mapper.readTree(response.body())).describedAs(what).isEqualTo(mapper.readTree(missing(catalog)))
    }

    private fun sourceHead(): Long = catalogs.getCatalog(source).headSnapshotId

    private fun teams(scan: JsonNode) = scan.map { it["team_id"].asLong() }.distinct().sorted()

    @Test
    fun `a reader gets the namespace, its tables, a table and a scoped scan`() {
        withHost { port ->
            val namespace = json(get(port, schema, reader()))
            assertThat(namespace["name"].asText()).isEqualTo("posthog")
            assertThat(namespace.has("containment")).isFalse()

            // Every table that has the team field, under its real name; the two that cannot be scoped are left out.
            val tables = json(get(port, "$schema/tables", reader()))
            assertThat(tables.map { it["name"].asText() }).containsExactly("events", "persons")
            val summary = tables.single { it["name"].asText() == "events" }
            // Teams 7 and 42 within the org's 7, 42, 49: files 1, 2 and 3. Not 4 (another org), 5 (pending), 6.
            assertThat(summary["file_count"].asLong()).isEqualTo(3)
            assertThat(summary["record_count"].asLong()).isEqualTo(600)
            assertThat(summary["table_uuid"].asText()).isEqualTo(events.tableUuid.toString())
            assertThat(tables.single { it["name"].asText() == "persons" }["file_count"].asLong()).isEqualTo(1)

            val table = json(get(port, "$schema/tables/events", reader()))
            assertThat(table["name"].asText()).isEqualTo("events")
            assertThat(table["namespace"].asText()).isEqualTo("posthog")
            assertThat(table.has("record_count")).isFalse()

            val scan = json(get(port, "$schema/tables/events/scan", reader()))
            assertThat(scan.map { it["data_file"]["data_file_id"].asLong() }).containsExactly(1L, 2L, 3L)
            scan.forEach { assertThat(it["data_file"]["path"].asText()).contains("team_id=${it["team_id"].asLong()}/") }
            // The live deletion vector rides its file.
            assertThat(
                scan.single { it["data_file"]["data_file_id"].asLong() == 1L }["delete_file"]["delete_count"].asLong(),
            )
                .isEqualTo(3)
            assertThat(json(get(port, "$schema/tables/persons/scan", reader())).map { it["team_id"].asLong() })
                .containsExactly(7L)
        }
    }

    @Test
    fun `every caller the rule refuses gets the identical 404 and nothing is read`() {
        withHost { port ->
            lookups.clear()
            statements.set(0)
            counting = true
            try {
                for ((what, token) in refused()) {
                    for (path in four) assertMissing(get(port, path, token), "$what on $path")
                }
            } finally {
                counting = false
            }
            assertThat(lookups).describedAs("no org's record was looked up").isEmpty()
            assertThat(statements.get()).describedAs("no statement was sent").isEqualTo(0)
            // The same callers on a query the scan route would refuse as invalid: still the 404.
            for ((what, token) in refused()) {
                assertMissing(get(port, "$schema/tables/events/scan?include=nonsense", token), what)
            }
            // With no token there is no caller to hold to the rule: the listener's 401, as on every route.
            assertThat(get(port, schema, null).statusCode()).isEqualTo(401)
        }
    }

    @Test
    fun `the rule holds with auth off and under warn`() {
        withHost(authMode = "off") { port ->
            // Off verifies no one, so no one is served: the reader's own token included.
            val readNothing =
                counted {
                    for (token in listOf(null, reader(), operator())) {
                        for (path in four) assertMissing(get(port, path, token), "off on $path")
                        assertMissing(get(port, "$schema/tables/events/scan?include=nonsense", token), "off, invalid")
                        assertMissing(send(port, "POST", "$schema/tables", token), "off, a write")
                    }
                }
            assertThat(readNothing).describedAs("off: record lookups, statements").isEqualTo(0 to 0)
        }
        withHost(authMode = "warn") { port ->
            // Warn lets a request with no token through to the handler, where the rule refuses it.
            val readNothing =
                counted {
                    for (path in four) assertMissing(get(port, path, null), "warn, no token, on $path")
                    for ((what, token) in refused()) {
                        for (path in four) assertMissing(get(port, path, token), "warn, $what, on $path")
                    }
                }
            assertThat(readNothing).describedAs("warn: record lookups, statements").isEqualTo(0 to 0)
            assertThat(teams(json(get(port, "$schema/tables/events/scan", reader())))).containsExactly(7L, 42L)
        }
    }

    /** The record lookups and the statements [requests] caused. */
    private fun counted(requests: () -> Unit): Pair<Int, Int> {
        lookups.clear()
        statements.set(0)
        sqls.clear()
        counting = true
        try {
            requests()
        } finally {
            counting = false
        }
        return lookups.size to statements.get()
    }

    @Test
    fun `column statistics are read for the scoped files only`() {
        withHost { port ->
            lateinit var scan: JsonNode
            val both = "$schema/tables/events/scan?include=column_stats,split_offsets"
            counted { scan = json(get(port, both, reader())) }
            val statsStatements = sqls.filter { "hog_file_column_stats" in it }
            assertThat(scan.map { it["data_file"]["data_file_id"].asLong() }).containsExactly(1L, 2L, 3L)
            scan.forEach { assertThat(it["data_file"]["column_stats"]).describedAs(it.toString()).hasSize(1) }
            assertThat(scan.first()["data_file"]["split_offsets"].map { it.asLong() }).containsExactly(4L, 100L)
            // One statement, entered from the plan's own file ids: never the whole table's statement.
            assertThat(statsStatements).hasSize(1)
            assertThat(statsStatements.single()).contains("s.data_file_id = ANY(:fileIds)")
            assertThat(
                statsStatements.single().trim(),
            ).isEqualTo(FileRepo.scopedColumnStatsSql(filtered = false).trim())
            // Narrowed by field, and without the option: no statistics statement at all.
            counted {
                json(
                    get(port, "$schema/tables/events/scan?include=column_stats&stats_fields=$teamFieldId", reader()),
                )
            }
            assertThat(sqls.filter { "hog_file_column_stats" in it }.map { it.trim() })
                .containsExactly(FileRepo.scopedColumnStatsSql(filtered = true).trim())
            counted { json(get(port, "$schema/tables/events/scan", reader())) }
            assertThat(sqls).noneMatch { "hog_file_column_stats" in it }
            // The legacy mount's scan reads through the same statement and answers as before.
            val mountScan =
                json(
                    get(
                        port,
                        "/v1/catalogs/$org-events/namespaces/posthog/tables/events/scan?include=column_stats",
                        mountReader(),
                    ),
                )
            assertThat(mountScan.map { it["data_file"]["column_stats"].size() }).containsExactly(1, 1, 1)
        }
        // The statement returns the rows of the files it is given and no others. File 4 is the other
        // org's, in the same table, with statistics of its own, and the whole-table statement returns it.
        db.jdbi.useHandle<Exception> { h ->
            val head = sourceHead()
            assertThat(FileRepo.providedColumnStatsAt(h, sourceCatalogId, events.tableId, head, null).keys)
                .containsExactlyInAnyOrder(1L, 2L, 3L, 4L)
            assertThat(FileRepo.providedColumnStatsOf(h, sourceCatalogId, events.tableId, listOf(1L, 3L), null).keys)
                .containsExactlyInAnyOrder(1L, 3L)
            // A file of another table is not returned, even when it is named.
            assertThat(
                FileRepo.providedColumnStatsOf(h, sourceCatalogId, events.tableId + 1, listOf(1L), null),
            ).isEmpty()
            assertThat(FileRepo.providedColumnStatsOf(h, sourceCatalogId, events.tableId, emptyList(), null)).isEmpty()
        }
    }

    @Test
    fun `a served scan keeps the file budget, the deadline and the shared cap, and the listing takes the cap`() {
        withHost(fileBudget = 1) { port ->
            val over = get(port, "$schema/tables/events/scan", reader())
            assertThat(over.statusCode()).isEqualTo(422)
            assertThat(code(over)).isEqualTo("mount_scan_too_many_files")
            // One file is within it.
            assertThat(json(get(port, "$schema/tables/persons/scan", reader()))).hasSize(1)
        }
        withHost(deadlineMs = 300) { port ->
            // The scan's statement waits on a lock another session holds, past the deadline.
            db.jdbi.useTransaction<Exception> { h ->
                h.execute("LOCK TABLE hog_data_file IN ACCESS EXCLUSIVE MODE")
                val late = get(port, "$schema/tables/events/scan", reader())
                assertThat(late.statusCode()).describedAs(late.body()).isEqualTo(503)
                assertThat(code(late)).isEqualTo("mount_scan_timeout")
            }
            assertThat(get(port, "$schema/tables/events/scan", reader()).statusCode()).isEqualTo(200)
        }
        withHost(maxConcurrent = 1, fresh = true) { port ->
            val cap = currentApp.mountReadServiceForTest.scanCap!!
            cap.acquire()
            try {
                for (path in listOf("$schema/tables/events/scan", "$schema/tables")) {
                    val busy = get(port, path, reader())
                    assertThat(busy.statusCode()).describedAs(path).isEqualTo(503)
                    assertThat(code(busy)).describedAs(path).isEqualTo("mount_scan_busy")
                    assertThat(busy.headers().firstValue("Retry-After")).isPresent()
                }
                // A refused caller is refused before the cap is looked at.
                assertMissing(get(port, "$schema/tables", operator()), "cap full, refused caller")
            } finally {
                cap.release()
            }
            // Both give the permit back.
            repeat(3) { assertThat(get(port, "$schema/tables", reader()).statusCode()).isEqualTo(200) }
            assertThat(cap.availablePermits()).isEqualTo(1)
            val reasons =
                get(port, "/metrics", operator()).body().lines()
                    .filter { it.startsWith("hoglake_mount_refusals_total{") && "mount_scan_busy" in it }
            assertThat(reasons.single()).contains("""catalog="posthog"""")
        }
    }

    @Test
    fun `snapshot and at_timestamp are the source's for a reader, and change nothing for a refused caller`() {
        val head = sourceHead()
        val now = java.time.Instant.now().toString()
        val queries =
            listOf("snapshot=$head", "snapshot=0", "snapshot=${head + 1000}", "snapshot=abc", "at_timestamp=$now") +
                listOf("at_timestamp=never", "snapshot=$head&at_timestamp=$now")
        withHost { port ->
            // At the head, by id or by time, the answer is the default one.
            for (path in four) {
                val plain = get(port, path, reader())
                assertThat(get(port, "$path?snapshot=$head", reader()).body()).describedAs(path).isEqualTo(plain.body())
                assertThat(
                    get(port, "$path?at_timestamp=$now", reader()).body(),
                ).describedAs(path).isEqualTo(plain.body())
            }
            // Before the namespace existed there is nothing to serve, and a bad query is the reader's own 4xx.
            assertMissing(get(port, "$schema?snapshot=0", reader()), "snapshot 0")
            for (bad in listOf(
                "snapshot=${head + 1000}",
                "snapshot=abc",
                "at_timestamp=never",
                "snapshot=$head&at_timestamp=$now",
            )) {
                for (path in four) {
                    val response = get(port, "$path?$bad", reader())
                    assertThat(response.statusCode()).describedAs("$path?$bad: ${response.body()}").isBetween(400, 422)
                    assertThat(response.body()).doesNotContain(source)
                }
            }
            // A refused caller gets the one 404 whatever the query says, and nothing is read.
            val readNothing =
                counted {
                    for ((what, token) in refused()) {
                        for (path in four) for (query in queries) assertMissing(
                            get(port, "$path?$query", token),
                            "$what $query",
                        )
                    }
                }
            assertThat(readNothing).isEqualTo(0 to 0)
        }
        withHost(authMode = "off") { port ->
            for (path in four) for (query in queries) assertMissing(get(port, "$path?$query", reader()), "off $query")
        }
    }

    @Test
    fun `with the real record store, off the allowlist, disabled, or the switch off reads nothing`() {
        val star = """{"team_id":{"*":"read"}}"""

        fun scanOf(
            port: Int,
            who: String,
        ) = json(
            get(port, "/v1/catalogs/$who/namespaces/posthog/tables/events/scan", reader(tokenOrg = who, prd = star)),
        )

        fun files(
            port: Int,
            who: String,
        ) = json(get(port, "/v1/catalogs/$who/namespaces/posthog/tables", reader(tokenOrg = who, prd = star)))
            .sumOf { it["file_count"].asLong() }
        // `hog_mount` holds an enabled record for the org and for the other org, and a disabled one.
        withHost(allowlist = setOf(org, disabledOrg)) { port ->
            assertThat(teams(scanOf(port, org))).containsExactly(7L, 42L)
            assertThat(files(port, org)).isEqualTo(4)
            // The other org's record is enabled, and its org is not on the allowlist.
            assertThat(scanOf(port, otherOrg)).isEmpty()
            assertThat(files(port, otherOrg)).isEqualTo(0)
            assertThat(scanOf(port, disabledOrg)).isEmpty()
            assertThat(files(port, disabledOrg)).isEqualTo(0)
            assertThat(scanOf(port, noRecordOrg)).isEmpty()
            // The instance switch off: the org that read a moment ago reads nothing.
            db.jdbi.useHandle<Exception> { it.execute("UPDATE hog_mount_switch SET enabled = false") }
            try {
                assertThat(scanOf(port, org)).isEmpty()
                assertThat(files(port, org)).isEqualTo(0)
            } finally {
                db.jdbi.useHandle<Exception> { it.execute("UPDATE hog_mount_switch SET enabled = true") }
            }
            assertThat(teams(scanOf(port, org))).containsExactly(7L, 42L)
        }
        // On the allowlist, the other org reads its own teams and only those.
        withHost(allowlist = setOf("*")) { port -> assertThat(teams(scanOf(port, otherOrg))).containsExactly(99L) }
        // With no allowlist at all, nobody's record is read.
        withHost(allowlist = emptySet()) { port -> assertThat(scanOf(port, org)).isEmpty() }
    }

    @Test
    fun `the public listener serves a reader and refuses everyone else`() {
        withHost(authMode = "warn", publicListener = true) { port ->
            assertThat(teams(json(get(port, "$schema/tables/events/scan", reader())))).containsExactly(7L, 42L)
            for (path in four) assertThat(get(port, path, reader()).statusCode()).describedAs(path).isEqualTo(200)
            for ((what, token) in refused().filterKeys { "operator" !in it }) {
                for (path in four) assertMissing(get(port, path, token), "public, $what, on $path")
            }
            // The listener drops an operator token and always enforces: its own 401, as on every route.
            for (token in listOf(operator(), null)) {
                for (path in four) assertThat(get(port, path, token).statusCode()).describedAs(path).isEqualTo(401)
            }
            assertThat(code(send(port, "POST", "$schema/tables", reader()))).isEqualTo("read_only_catalog")
        }
    }

    @Test
    fun `a placement on a backend the Host does configure is ignored too`() {
        val backend = "6e3f1d2c-4b5a-4d6e-9f7a-8b9c0d1e2f3a"
        val rtg = ""","rtg":{"b":"postgres","be":"$backend"}"""
        withHost(postgresBackendId = backend) { port ->
            val routed = reader(extra = rtg)
            for (path in four) assertThat(get(port, path, routed).statusCode()).describedAs(path).isEqualTo(200)
            assertThat(get(port, "$schema/tables/events/scan", routed).body())
                .isEqualTo(get(port, "$schema/tables/events/scan", reader()).body())
            assertMissing(get(port, schema, reader(tokenOrg = otherOrg, cat = org, extra = rtg)), "routed, other org")
            // Elsewhere the placement verifies and routes as before: the Host holds no catalog of that name.
            val elsewhere = get(port, "/v1/catalogs/$org/namespaces/ingest/tables", routed)
            assertThat(elsewhere.statusCode()).isEqualTo(404)
            assertThat(code(elsewhere)).isEqualTo("not_found")
        }
    }

    @Test
    fun `a table that cannot be scoped is left out of the listing and counted, not a 500 for every org`() {
        withHost(fresh = true) { port ->
            // `broken` names the team field in its layout and has no partition spec on it.
            for (who in listOf(org, otherOrg)) {
                val tables = get(port, "/v1/catalogs/$who/namespaces/posthog/tables", reader(tokenOrg = who))
                assertThat(
                    json(tables).map { it["name"].asText() },
                ).describedAs(who).containsExactly("events", "persons")
            }
            // Its scan still refuses: nothing proves which team its files belong to.
            assertThat(get(port, "$schema/tables/broken/scan", reader()).statusCode()).isEqualTo(500)
            val series = get(port, "/metrics", operator()).body().lines()
            val counted = series.single { it.startsWith("hoglake_served_schema_unscopable_tables_total{") }
            assertThat(counted).contains("""schema="posthog"""")
            assertThat(counted.substringAfterLast(' ').toDouble()).isGreaterThanOrEqualTo(2.0)
            assertThat(
                series.filter { it.startsWith("hoglake_served_schema_") },
            ).noneMatch { org in it || otherOrg in it }
        }
    }

    @Test
    fun `a Host that holds an org shaped catalog refuses to boot`() {
        val other = PgTestSupport.freshDatabase()
        try {
            val service = CatalogService(other.jdbi)
            service.createCatalog(source, "s3://bucket/millrace/$source")
            val cfg = Config(port = 0, hydratorIntervalMs = 0).copy(servedSchemasFile = servedFile(), authMode = "off")
            // Without such a catalog it boots.
            App.build(cfg, other.jdbi).close()
            service.createCatalog(org, "s3://bucket/customer/$org")
            assertThatThrownBy { App.build(cfg, other.jdbi).close() }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("this server holds the catalog '$org'")
            // The same server without the setting is not asked.
            App.build(cfg.copy(servedSchemasFile = ""), other.jdbi).close()
        } finally {
            other.close()
        }
    }

    @Test
    fun `a star grant reads exactly the org's teams`() {
        withHost { port ->
            val star = reader(prd = """{"team_id":{"*":"read"}}""")
            // The org's teams are 7, 42 and 49 (49 has no file). Team 99 is in the table and is another org's.
            assertThat(teams(json(get(port, "$schema/tables/events/scan", star)))).containsExactly(7L, 42L)
            assertThat(
                json(get(port, "$schema/tables/events/scan", star)).map { it["data_file"]["data_file_id"].asLong() },
            )
                .containsExactly(1L, 2L, 3L)
            assertThat(teams(json(get(port, "$schema/tables/persons/scan", star)))).containsExactly(7L)
            // The other org's star reads its own teams of the same tables, and none of this org's.
            val other = reader(tokenOrg = otherOrg, prd = """{"team_id":{"*":"read"}}""")
            val otherSchema = "/v1/catalogs/$otherOrg/namespaces/posthog"
            assertThat(teams(json(get(port, "$otherSchema/tables/events/scan", other)))).containsExactly(99L)
            assertThat(teams(json(get(port, "$otherSchema/tables/persons/scan", other)))).containsExactly(500L)
        }
    }

    @Test
    fun `an explicit grant outside the org's teams reads none of them`() {
        withHost { port ->
            // 99 and 500 are real teams with files in these tables, and belong to the other org.
            val outside = reader(prd = """{"team_id":{"99":"read","500":"read"}}""")
            assertThat(json(get(port, "$schema/tables/events/scan", outside))).isEmpty()
            assertThat(json(get(port, "$schema/tables/persons/scan", outside))).isEmpty()
            json(get(port, "$schema/tables", outside)).forEach {
                assertThat(it["file_count"].asLong()).describedAs(it["name"].asText()).isEqualTo(0)
                assertThat(it["record_count"].asLong()).isEqualTo(0)
            }
            // A grant that mixes the two reads only the org's own team.
            val mixed = reader(prd = """{"team_id":{"7":"read","99":"read"}}""")
            assertThat(teams(json(get(port, "$schema/tables/events/scan", mixed)))).containsExactly(7L)
        }
    }

    @Test
    fun `an org with no record, or a disabled one, reads no file`() {
        withHost { port ->
            val grants = listOf("""{"team_id":{"*":"read"}}""", """{"team_id":{"7":"read","42":"read"}}""")
            for (empty in listOf(noRecordOrg, disabledOrg)) for (prd in grants) {
                val token = reader(tokenOrg = empty, prd = prd)
                val path = "/v1/catalogs/$empty/namespaces/posthog"
                assertThat(json(get(port, "$path/tables/events/scan", token))).describedAs(empty).isEmpty()
                assertThat(json(get(port, "$path/tables/persons/scan", token))).describedAs(empty).isEmpty()
                json(get(port, "$path/tables", token)).forEach {
                    assertThat(it["file_count"].asLong()).describedAs(empty).isEqualTo(0)
                    assertThat(it["file_size_bytes"].asLong()).isEqualTo(0)
                }
                // The table's definition is not the org's data, and is still served.
                assertThat(json(get(port, "$path/tables/events", token))["name"].asText()).isEqualTo("events")
            }
        }
    }

    @Test
    fun `rtg is ignored on a served path and still refused everywhere else`() {
        // A placement on a backend this server does not configure: the Client's own.
        val rtg = ""","rtg":{"b":"hoglite","be":"5d2f0c1e-3a4b-4c5d-8e6f-7a8b9c0d1e2f","t":"t_org_1"}"""
        withHost { port ->
            val routed = reader(extra = rtg)
            assertThat(teams(json(get(port, "$schema/tables/events/scan", routed)))).containsExactly(7L, 42L)
            for (path in four) assertThat(get(port, path, routed).statusCode()).describedAs(path).isEqualTo(200)
            // Any other path verifies the placement as before.
            val elsewhere = get(port, "/v1/catalogs/$org/namespaces", routed)
            assertThat(elsewhere.statusCode()).isEqualTo(401)
            assertThat(code(elsewhere)).isEqualTo("unknown_backend")
            // A malformed placement refuses the token on a served path too.
            val malformed = reader(extra = ""","rtg":{"b":"hoglite"}""")
            assertThat(get(port, schema, malformed).statusCode()).isEqualTo(401)
            // A refused caller's placement does not change its answer.
            assertMissing(get(port, schema, reader(schemas = null, extra = rtg)), "routed, no claim")
        }
        withHost(served = false) { port ->
            assertThat(code(get(port, schema, reader(extra = rtg)))).isEqualTo("unknown_backend")
        }
    }

    @Test
    fun `nothing outside the served namespace is reachable by name`() {
        withHost { port ->
            val token = reader(prd = """{"team_id":{"*":"read"}}""")
            // A table of another namespace of the source catalog, named through the served one.
            for (tail in listOf("secret_events", "secret_events/scan")) {
                val response = get(port, "$schema/tables/$tail", token)
                assertThat(response.statusCode()).describedAs(tail).isEqualTo(404)
                assertThat(response.body()).contains("table 'posthog.secret_events'")
            }
            // The tables of the served namespace that cannot be scoped.
            for (table in listOf("no_layout", "by_day", "absent")) {
                assertThat(get(port, "$schema/tables/$table", token).statusCode()).describedAs(table).isEqualTo(404)
                assertThat(
                    get(port, "$schema/tables/$table/scan", token).statusCode(),
                ).describedAs(table).isEqualTo(404)
            }
            // The source catalog's other namespace, and a case variant, are not served paths: no such catalog.
            for (namespace in listOf("ingest", "PostHog")) {
                val path = "/v1/catalogs/$org/namespaces/$namespace"
                for (tail in listOf("", "/tables", "/tables/secret_events", "/tables/secret_events/scan")) {
                    val response = get(port, "$path$tail", token)
                    assertThat(response.statusCode()).describedAs("$namespace$tail").isEqualTo(404)
                    assertThat(response.body()).doesNotContain("team_id", source)
                }
            }
            // The source catalog by its own name is not this reader's.
            assertThat(get(port, "/v1/catalogs/$source/namespaces/posthog/tables", token).statusCode()).isEqualTo(404)
            // No answer on the served path names the source catalog's other namespace.
            for (path in four) assertThat(get(port, path, token).body()).doesNotContain("ingest", "secret_events")
        }
    }

    @Test
    fun `every other route on the served namespace is refused and writes nothing`() {
        val routes = RemoteSchemaFixtures.namespaceRoutes(org).filterNot { RemoteSchemaFixtures.isRelayed(it.first) }
        assertThat(routes).hasSizeGreaterThan(15)
        val before = sourceHead()
        for (mode in listOf("enforce", "off")) {
            withHost(authMode = mode) { port ->
                for ((rule, path) in routes) {
                    val method = rule.method.value
                    val what = "$mode $method ${rule.pattern}"
                    // No handler reads or writes anything for a refused caller: off has no plugin in front of it.
                    lookups.clear()
                    statements.set(0)
                    counting = true
                    try {
                        for ((who, token) in refused()) assertMissing(send(port, method, path, token), "$who: $what")
                    } finally {
                        counting = false
                    }
                    assertThat(lookups).describedAs(what).isEmpty()
                    assertThat(statements.get()).describedAs("statements sent for %s", what).isEqualTo(0)
                    val entitled = send(port, method, path, reader())
                    when {
                        // Off verifies no one, so the reader is refused like everyone else.
                        mode == "off" -> assertMissing(entitled, what)
                        rule.requirement == Requirement.READ -> {
                            assertThat(
                                entitled.statusCode(),
                            ).describedAs("%s: %s", what, entitled.body()).isEqualTo(501)
                            assertThat(code(entitled)).describedAs(what).isEqualTo("mount_operation_unavailable")
                        }
                        else -> {
                            assertThat(
                                entitled.statusCode(),
                            ).describedAs("%s: %s", what, entitled.body()).isEqualTo(403)
                            assertThat(code(entitled)).describedAs(what).isEqualTo("read_only_catalog")
                        }
                    }
                }
            }
        }
        assertThat(sourceHead()).isEqualTo(before)
    }

    @Test
    fun `the source catalog keeps its own routes for its own callers`() {
        withHost { port ->
            // An operator reads and writes the served namespace under the source catalog's name, as before.
            val own = "/v1/catalogs/$source/namespaces/posthog"
            assertThat(json(get(port, "$own/tables", operator())).map { it["name"].asText() })
                .containsExactly("broken", "by_day", "events", "no_layout", "persons")
            assertThat(get(port, "$own/tables/events/scan", operator()).statusCode()).isEqualTo(200)
            assertThat(
                get(port, "/v1/catalogs/$source/namespaces/ingest/tables", operator()).statusCode(),
            ).isEqualTo(200)
        }
    }

    @Test
    fun `the legacy mount is unchanged beside a served schema`() {
        val mount = "$org-events"
        val iat = System.currentTimeMillis() / 1000 - 5
        val mountReader =
            TestTokens.mint(
                pair,
                TestTokens.scopedClaims(
                    prd = """{"team_id":{"7":"read","42":"read"}}""",
                    catalogs = """{"$mount":["read"]}""",
                    iat = iat,
                    org = org,
                ),
            )
        val answers =
            listOf(true, false).map { served ->
                withHost(served = served) { port ->
                    listOf(
                        "/v1/catalogs/$mount",
                        "/v1/catalogs/$mount/namespaces",
                        "/v1/catalogs/$mount/namespaces/posthog",
                        "/v1/catalogs/$mount/namespaces/posthog/tables",
                        "/v1/catalogs/$mount/namespaces/posthog/tables/events",
                        "/v1/catalogs/$mount/namespaces/posthog/tables/events/scan",
                        "/v1/catalogs/$mount/namespaces/posthog/tables/persons",
                    ).map { path -> get(port, path, mountReader).let { it.statusCode() to it.body() } } +
                        // A served schema's reader is no mount reader, and a mount's reader reads no served schema.
                        get(port, "/v1/catalogs/$mount/namespaces/posthog", reader())
                            .let { it.statusCode() to it.body() } +
                        send(port, "POST", "/v1/catalogs/$mount/namespaces/posthog/tables", mountReader)
                            .let { it.statusCode() to it.body() }
                }
            }
        assertThat(answers[0]).isEqualTo(answers[1])
        assertThat(answers[0].map { it.first }).containsExactly(200, 200, 200, 200, 200, 200, 404, 404, 403)
        withHost { port -> assertMissing(get(port, schema, mountReader), "a mount's reader on the served path") }
    }

    @Test
    fun `a server without the setting answers as before`() {
        withHost(served = false) { port ->
            lookups.clear()
            // No catalog is named by the org's id here, so the path is a missing catalog's for everyone.
            for (token in listOf(reader(), operator(), tenant())) {
                for (path in four) {
                    val response = get(port, path, token)
                    assertThat(response.statusCode()).describedAs(path).isEqualTo(404)
                    assertThat(response.body()).describedAs(path).doesNotContain("events", "team_id")
                }
            }
            assertThat(lookups).describedAs("the org's record is never read").isEmpty()
        }
    }

    @Test
    fun `a second hoglake relays its readers to the Host, under an alias too`() {
        // The Client: its own database, holding the org's catalog, with the Host's schema shown as `ph`.
        val clientDb = PgTestSupport.freshDatabase()
        try {
            CatalogService(clientDb.jdbi).createCatalog(org, "s3://bucket/customer/$org")
            withHost { hostPort ->
                val hosts =
                    Files.createTempFile("hoglake-served-schema-hosts-", ".json").also {
                        it.toFile().deleteOnExit()
                        Files.writeString(
                            it,
                            """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","match":{"type":"namespace",""" +
                                """"name":"ph"},"remote_name":"posthog","kind":"hoglake",""" +
                                """"url":"http://127.0.0.1:$hostPort","auth":"forward","allow_insecure_http":true}]}""",
                        )
                    }
                val cfg =
                    Config(port = 0, hydratorIntervalMs = 0).copy(
                        authMode = "enforce",
                        authIssuer = TestTokens.ISSUER,
                        authJwksFile = keyFile.toString(),
                        authPredicateFields = "team_id",
                        authSessionProfile = "on",
                        mountHostsFile = hosts.toString(),
                    )
                val app = App.build(cfg, clientDb.jdbi)
                val client = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
                try {
                    client.start(wait = false)
                    val port = runBlocking { client.engine.resolvedConnectors().first().port }
                    val local = "/v1/catalogs/$org/namespaces/ph"
                    // The alias is the Client's; the Host sees and checks its own name.
                    assertThat(json(get(port, local, reader()))["name"].asText()).isEqualTo("ph")
                    assertThat(json(get(port, "$local/tables", reader())).map { it["name"].asText() })
                        .containsExactly("events", "persons")
                    assertThat(json(get(port, "$local/tables/events", reader()))["namespace"].asText()).isEqualTo("ph")
                    val relayed = get(port, "$local/tables/events/scan", reader(prd = """{"team_id":{"*":"read"}}"""))
                    val direct =
                        get(hostPort, "$schema/tables/events/scan", reader(prd = """{"team_id":{"*":"read"}}"""))
                    assertThat(teams(json(relayed))).containsExactly(7L, 42L)
                    assertThat(relayed.body()).isEqualTo(direct.body())
                    // The Host's refusal of a token the Client let through comes back as the Host wrote it.
                    val foreign = get(port, "$local/tables/events/scan", reader(tokenOrg = otherOrg, cat = org))
                    assertThat(foreign.statusCode()).isEqualTo(404)
                    assertThat(mapper.readTree(foreign.body())).isEqualTo(mapper.readTree(missing()))
                } finally {
                    client.stop(0, 1000)
                    app.close()
                }
            }
        } finally {
            clientDb.close()
        }
    }

    @Test
    fun `a served schema whose source catalog does not exist refuses to boot`() {
        val file =
            Files.createTempFile("hoglake-served-schemas-", ".json").also {
                it.toFile().deleteOnExit()
                Files.writeString(it, """{"schemas":[{"name":"posthog","catalog":"absent"}]}""")
            }
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                servedSchemasFile = file.toString(),
                authMode = "off",
            )
        assertThatThrownBy { App.build(cfg, db.jdbi).close() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("the catalog 'absent' of the served schema 'posthog' does not exist")
    }

    @Test
    fun `the series are labelled by the schema's name and never by an org`() {
        withHost(fresh = true) { port ->
            get(port, "$schema/tables/events/scan", reader())
            get(port, schema, operator())
            val series =
                get(port, "/metrics", operator()).body().lines().filter { it.startsWith("hoglake_mount_") }
            assertThat(series.single { it.startsWith("hoglake_mount_scans_total{") }).contains("""catalog="posthog"""")
            assertThat(series.filter { it.startsWith("hoglake_mount_refusals_total{") })
                .anyMatch { """catalog="posthog"""" in it && """reason="not_found"""" in it }
            assertThat(series).noneMatch { org in it || otherOrg in it }
        }
    }
}
