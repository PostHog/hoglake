package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
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
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The stream half of a served `team_<id>` on the Host end to end (docs/auth-tenancy.md §6.5d): the real
 * application over PostgreSQL. The imports catalog holds team namespaces marked for org A (42, 43, 48), for org B
 * (77), and marked wrongly (44, 45). The stream catalog's `ingest` namespace holds the published tables, shared by
 * every team: `events` and `persons` laid out on `team_id` with files of teams 42, 43, 48 and 77, and published
 * names that cannot be served. It also holds the unpublished `events_raw`; another namespace holds tables of the
 * same layout, one under a published name. The callers' catalog, `cust`, does not exist on this server.
 *
 * Every refusal is checked for its answer and for what it read: no statement names the stream catalog or reads a
 * file row.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServedStreamIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-served-stream-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val orgA = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val orgB = "02b2f6ef-2bd6-0000-f64f-e3a0b88efbf5"
    private val imports = "imports-dev"
    private val streamCatalog = "millrace-dev"
    private val streamNamespace = "ingest"
    private val cust = "cust"
    private val published = listOf("events", "persons", "sessions", "gone", "by_day", "no_layout", "broken")

    /** The legacy `<orgID>-events` mount, over the same `events` table. */
    private val mount = "$orgA-events"
    private val ns = "/v1/catalogs/$cust/namespaces/team_42"
    private val four = listOf(ns, "$ns/tables", "$ns/tables/events", "$ns/tables/events/scan")
    private val importFile = "s3://bucket/$imports/org_id=$orgA/team_id=42/pageviews/a.parquet"
    private val shadowedFile = "s3://bucket/$imports/org_id=$orgA/team_id=43/events/shadow.parquet"
    private val unlisted = """{"b":"hoglite","be":"5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","t":"opaque_1"}"""

    private val events: TableInfo
    private val teamFieldId: Long

    /** Before the stream namespace existed, and the import catalog's head before `invoices` was created. */
    private val beforeStream: Instant
    private val importsBeforeInvoices: Long

    /** The `history` namespace's early snapshot, and a time between it and the later one. */
    private val historyEarly: Long
    private val historyEarlyAt: Instant

    @Volatile
    private var capturing = false

    /** Every statement's text and binding sent while [capturing] is on. */
    private val sqls = CopyOnWriteArrayList<String>()

    /** Called with each statement's text before it runs, while set: for a test that moves the head mid-request. */
    @Volatile
    private var beforeStatement: ((String) -> Unit)? = null

    init {
        catalogs.createCatalog(imports, "s3://bucket/$imports")
        val marks =
            mapOf(
                "team_42" to NamespaceContainment("team_id", 42, orgA),
                "team_43" to NamespaceContainment("team_id", 43, orgA),
                "team_48" to NamespaceContainment("team_id", 48, orgA),
                "team_77" to NamespaceContainment("team_id", 77, orgB),
                // Marked for another team than its name says, and for another field than the served one.
                "team_44" to NamespaceContainment("team_id", 99, orgA),
                "team_45" to NamespaceContainment("project_id", 45, orgA),
            )
        for ((name, mark) in marks) {
            catalogs.createNamespace(imports, name)
            catalogs.setNamespaceContainment(imports, name, mark)
            // team_48 has no import table at all.
            if (name != "team_48") {
                catalogs.createTable(
                    imports,
                    name,
                    "pageviews",
                    listOf(ColumnDef("v", ColType.LONG)),
                )
            }
        }
        // Imports named as a stream table, exactly and in another case.
        catalogs.createTable(imports, "team_43", "events", listOf(ColumnDef("v", ColType.LONG)))
        catalogs.createTable(imports, "team_43", "EVENTS", listOf(ColumnDef("v", ColType.LONG)))
        commits.commit(
            imports,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "team_42",
                            "pageviews",
                            listOf(FileRegistration(importFile, 10, 1_000, 20, null, null)),
                        ),
                        TableAppend(
                            "team_43",
                            "events",
                            listOf(FileRegistration(shadowedFile, 10, 1_000, 20, null, null)),
                        ),
                    ),
            ),
        )
        catalogs.createCatalog(streamCatalog, "s3://bucket/millrace/$streamCatalog")
        Thread.sleep(50)
        beforeStream = Instant.now()
        Thread.sleep(50)
        importsBeforeInvoices = catalogs.getCatalog(imports).headSnapshotId
        catalogs.createTable(imports, "team_42", "invoices", listOf(ColumnDef("v", ColType.LONG)))

        catalogs.createNamespace(streamCatalog, streamNamespace)
        catalogs.createNamespace(streamCatalog, "other")

        fun create(
            namespace: String,
            name: String,
            vararg columns: String,
        ) = catalogs.createTable(streamCatalog, namespace, name, columns.map { ColumnDef(it, ColType.LONG) })
        events = create(streamNamespace, "events", "team_id", "v")
        val persons = create(streamNamespace, "persons", "team_id")
        val raw = create(streamNamespace, "events_raw", "team_id")
        val byDay = create(streamNamespace, "by_day", "day", "team_id")
        val plain = create(streamNamespace, "no_layout", "team_id")
        val broken = create(streamNamespace, "broken", "team_id")
        val sessions = create("other", "sessions", "team_id")
        val secret = create("other", "secret", "team_id")
        teamFieldId = events.columns.single { it.def.name == "team_id" }.fieldId
        db.jdbi.useHandle<Exception> { h ->
            val catId = catalogId(h, streamCatalog)
            for (table in listOf(events, persons, raw, sessions, secret)) layout(h, catId, table, "team_id")
            layout(h, catId, byDay, "day")
            partitioned(h, catId, plain, "team_id")
            // A layout on the team field and no spec that partitions by it: it cannot be scoped.
            h.execute(
                "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                    "VALUES (?, ?, 1, ?)",
                catId,
                broken.tableId,
                broken.columns.single().fieldId,
            )
            file(h, catId, events, 1, "42")
            file(h, catId, events, 2, "42")
            file(h, catId, events, 3, "43")
            file(h, catId, events, 4, "77") // org B's team
            file(h, catId, events, 5, "42", state = "pending") // stats not proven
            file(h, catId, events, 6, "42", quarantined = true) // the footer did not prove the team
            file(h, catId, events, 15, "48")
            file(h, catId, persons, 7, "42")
            file(h, catId, persons, 8, "43")
            file(h, catId, persons, 9, "77")
            file(h, catId, raw, 10, "42")
            file(h, catId, sessions, 11, "42")
            file(h, catId, secret, 12, "42")
            file(h, catId, byDay, 13, "42")
            file(h, catId, plain, 14, "42")
            h.execute(
                "INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, " +
                    "begin_snapshot, end_snapshot, path, file_format, delete_count, file_size_bytes) " +
                    "VALUES (?, 9001, ?, 1, 1, NULL, ?, 'puffin-dv', 3, 128)",
                catId,
                events.tableId,
                "s3://bucket/millrace/$streamCatalog/data/$streamNamespace/events/team_id=42/dv1.puffin",
            )
            // Column statistics for team 42's files and for teams 43's and 77's, in the same table.
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
            h.execute(
                "UPDATE hog_data_file SET split_offsets = ARRAY[4, 100]::bigint[] " +
                    "WHERE catalog_id = ? AND data_file_id = 1",
                catId,
            )
            // The shared table's own metadata, which no team's reader is served.
            h.execute(
                "UPDATE hog_table_version SET properties = '{\"posthog.pipeline\":\"internal-topic\"}'::jsonb " +
                    "WHERE catalog_id = ? AND table_id = ?",
                catId,
                events.tableId,
            )
            h.execute("UPDATE hog_mount_switch SET enabled = true")
        }
        // A history of team 42's files in a namespace of its own, served by a Host whose stream block names it:
        // file 20 is live throughout; 22 is removed and 21 added at a later snapshot; 23 is team 43's.
        catalogs.createNamespace(streamCatalog, "history")
        val history = create("history", "events", "team_id")
        historyEarly = catalogs.getCatalog(streamCatalog).headSnapshotId
        db.jdbi.useHandle<Exception> { h ->
            val catId = catalogId(h, streamCatalog)
            layout(h, catId, history, "team_id")
            for ((id, team) in listOf(
                20L to "42",
                22L to "42",
                23L to "43",
            )) file(h, catId, history, id, team, historyEarly)
        }
        Thread.sleep(50)
        historyEarlyAt = Instant.now()
        Thread.sleep(50)
        create("history", "later", "v")
        val later = catalogs.getCatalog(streamCatalog).headSnapshotId
        db.jdbi.useHandle<Exception> { h ->
            val catId = catalogId(h, streamCatalog)
            file(h, catId, history, 21, "42", later)
            h.execute(
                "UPDATE hog_data_file SET end_snapshot = ? WHERE catalog_id = ? AND data_file_id = 22",
                later,
                catId,
            )
        }
        db.jdbi.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    beforeStatement?.invoke(context.rawSql)
                    if (capturing) sqls += "${context.rawSql} || bound=${context.binding}"
                }
            },
        )
    }

    private fun catalogId(
        h: Handle,
        name: String,
    ): Long =
        h.createQuery(
            "SELECT catalog_id FROM hog_catalog WHERE name = :n",
        ).bind("n", name).mapTo(Long::class.java).one()

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
        begin: Long = 1,
        state: String = "provided",
        quarantined: Boolean = false,
    ) {
        h.execute(
            "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, " +
                "end_snapshot, path, record_count, file_size_bytes, row_id_start, spec_id, " +
                "stats_state, prefix_quarantined) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, 1, ?, ?)",
            catId,
            id,
            table.tableId,
            begin,
            "s3://bucket/millrace/$streamCatalog/data/${table.namespace}/${table.name}/team_id=$team/f$id.parquet",
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

    // ---- hosts -------------------------------------------------------------

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

    @AfterAll
    fun close() {
        hosts.values.forEach { it.close() }
        hosts.clear()
        sqls.clear()
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    /**
     * A Host. [stream] null serves the imports alone; otherwise the stream block names that catalog. [fresh] builds
     * one for [block] alone, for a test that reads `/metrics` (which reports the app built last) or holds the cap.
     */
    private fun <T> withHost(
        stream: String? = streamCatalog,
        namespace: String = streamNamespace,
        authMode: String = "enforce",
        fileBudget: Int = 2_000_000,
        deadlineMs: Long = 60_000,
        maxConcurrent: Int = 8,
        fresh: Boolean = false,
        block: (port: Int, app: App) -> T,
    ): T {
        val key = listOf(stream, namespace, authMode, fileBudget, deadlineMs, maxConcurrent)
        val start = { startHost(stream, namespace, authMode, fileBudget, deadlineMs, maxConcurrent) }
        if (!fresh) {
            val host = hosts.getOrPut(key, start)
            return block(host.port, host.app)
        }
        start().use { return block(it.port, it.app) }
    }

    private fun startHost(
        stream: String?,
        namespace: String,
        authMode: String,
        fileBudget: Int,
        deadlineMs: Long,
        maxConcurrent: Int,
    ): RunningHost {
        val block =
            stream?.let {
                ""","stream":{"catalog":"$it","namespace":"$namespace",""" +
                    """"tables":${published.joinToString(",", "[", "]") { name -> "\"$name\"" }}}"""
            } ?: ""
        val served =
            Files.createTempFile("hoglake-served-stream-ns-", ".json").also {
                it.toFile().deleteOnExit()
                val entry = """{"family":"team","source_catalog":"$imports","predicate_field":"team_id"$block}"""
                Files.writeString(it, """{"families":[$entry]}""")
            }
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = authMode,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                authSessionProfile = "on",
                servedNamespacesFile = served.toString(),
                // The legacy mount, beside it, over the same events table.
                mountSourceCatalog = streamCatalog,
                mountSourceTableUuid = events.tableUuid.toString(),
                mountSourceFieldId = teamFieldId.toString(),
                mountScanFileBudget = fileBudget,
                mountScanDeadlineMs = deadlineMs,
                mountScanMaxConcurrent = maxConcurrent,
            )
        val app =
            App.build(
                cfg,
                db.jdbi,
                mountRegistry =
                    MountRegistry {
                        if (it == mount) {
                            MountRecord(
                                mount,
                                1,
                                true,
                                listOf(42, 43),
                            )
                        } else {
                            null
                        }
                    },
            )
        val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        server.start(wait = false)
        return RunningHost(
            app,
            runBlocking { server.engine.resolvedConnectors().first().port },
        ) { server.stop(0, 1000) }
    }

    // ---- callers -----------------------------------------------------------

    /** A session-profile scoped token as the issuer shapes it; [cat] replaces the `cat` claim verbatim. */
    private fun scoped(
        org: String = orgA,
        prd: String = """{"team_id":{"42":"read"}}""",
        schemas: String? = """{"team":["read"]}""",
        catalog: String = cust,
        scope: String = "read",
        rtg: String? = null,
        cat: String? = null,
    ): String =
        TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                prd = prd,
                catalogs = cat ?: """{"$catalog":["$scope"]}""",
                iat = System.currentTimeMillis() / 1000 - 5,
                extra = TestTokens.sessionProfile(catalog, schemas) + (rtg?.let { ""","rtg":$it""" } ?: ""),
                org = org,
            ),
        )

    private fun star(org: String = orgA) = scoped(org = org, prd = """{"team_id":{"*":"read"}}""")

    private fun tenant() =
        TestTokens.mint(
            pair,
            TestTokens.tenantClaims(org = orgA, catalogs = mapOf(cust to listOf("admin"))).dropLast(1) +
                TestTokens.sessionProfile(cust, """{"team":["read"]}""") + "}",
        )

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    // ---- requests and checks ------------------------------------------------

    private fun send(
        port: Int,
        method: String,
        path: String,
        token: String?,
        body: String? = null,
    ): HttpResponse<String> {
        val payload = body ?: if (method == "GET" || method == "DELETE") null else "{}"
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    payload?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
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

    private fun names(listing: JsonNode) = listing.map { it["name"].asText() }

    private fun ids(scan: JsonNode) = scan.map { it["data_file"]["data_file_id"].asLong() }.sorted()

    private fun teams(scan: JsonNode) = scan.map { it["team_id"].asLong() }.distinct().sorted()

    private fun path(
        team: String,
        suffix: String = "",
    ) = "/v1/catalogs/$cust/namespaces/$team$suffix"

    private fun relayed(team: String) =
        listOf("", "/tables", "/tables/events", "/tables/events/scan").map {
            path(team, it)
        }

    /** The one refusal of the caller rule: the 404 of a missing namespace, in words that depend on the path alone. */
    private fun assertMissing(
        response: HttpResponse<String>,
        what: String,
        team: String = "team_42",
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(404)
        assertThat(mapper.readTree(response.body())).describedAs(what)
            .isEqualTo(mapper.readTree("""{"error":"not_found","detail":"namespace '$team' in catalog '$cust'"}"""))
    }

    private fun assertMissingTable(
        response: HttpResponse<String>,
        table: String,
        team: String = "team_42",
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", table, response.body()).isEqualTo(404)
        assertThat(mapper.readTree(response.body())).describedAs(table)
            .isEqualTo(mapper.readTree("""{"error":"not_found","detail":"table '$team.$table' in catalog '$cust'"}"""))
    }

    /** The statements [requests] sent, each with its binding. */
    private fun captured(requests: () -> Unit): List<String> {
        sqls.clear()
        capturing = true
        try {
            requests()
        } finally {
            capturing = false
        }
        return sqls.toList()
    }

    /** [requests] read nothing of the stream: no statement names its catalog, and none reads a file or a layout. */
    private fun assertNothingRead(
        what: String,
        requests: () -> Unit,
    ) {
        val sent = captured(requests)
        assertThat(sent).describedAs("%s: a statement against the stream catalog", what)
            .noneMatch { streamCatalog in it }
        for (table in listOf("hog_data_file", "hog_file_partition_value", "hog_table_layout", "hog_table_version")) {
            assertThat(sent).describedAs("%s: a statement on %s", what, table).noneMatch { table in it }
        }
    }

    /** The column statistics statements among [sent], without their bindings. */
    private fun statistics(sent: List<String>) =
        sent.filter { "hog_file_column_stats" in it }.map { it.substringBefore(" || bound=").trim() }

    private fun streamHead() = catalogs.getCatalog(streamCatalog).headSnapshotId

    private fun importsHead() = catalogs.getCatalog(imports).headSnapshotId

    // ---- served ------------------------------------------------------------

    @Test
    fun `org A's reader of team 42 gets the namespace, the imports and the stream tables, and team 42's files`() {
        withHost { port, _ ->
            val namespace = json(get(port, ns, scoped()))
            assertThat(namespace["name"].asText()).isEqualTo("team_42")
            assertThat(namespace.has("containment")).isFalse()

            // The imports and the published tables that can be served, by name.
            val tables = json(get(port, "$ns/tables", scoped()))
            assertThat(names(tables)).containsExactly("events", "invoices", "pageviews", "persons")
            assertThat(tables.single { it["name"].asText() == "events" }["table_uuid"].asText())
                .isEqualTo(events.tableUuid.toString())

            val table = json(get(port, "$ns/tables/events", scoped()))
            assertThat(table["name"].asText()).isEqualTo("events")
            assertThat(table["namespace"].asText()).isEqualTo("team_42")
            assertThat(table["table_uuid"].asText()).isEqualTo(events.tableUuid.toString())
            assertThat(table["object_prefix_field"].asText()).isEqualTo("team_id")

            val scan = json(get(port, "$ns/tables/events/scan", scoped()))
            assertThat(ids(scan)).containsExactly(1L, 2L)
            scan.forEach {
                assertThat(it["team_id"].asLong()).isEqualTo(42L)
                assertThat(it["data_file"]["path"].asText()).contains("/team_id=42/")
            }
            val first = scan.single { it["data_file"]["data_file_id"].asLong() == 1L }
            assertThat(first["delete_file"]["delete_file_id"].asLong()).isEqualTo(9001L)
            assertThat(ids(json(get(port, "$ns/tables/persons/scan", scoped())))).containsExactly(7L)

            // The imports are read as before.
            assertThat(json(get(port, "$ns/tables/pageviews", scoped()))["namespace"].asText()).isEqualTo("team_42")
            assertThat(get(port, "$ns/tables/pageviews/scan", scoped()).body()).contains(importFile)
        }
    }

    @Test
    fun `a token holding two teams reads each team's stream rows in that team's schema only`() {
        val both = scoped(prd = """{"team_id":{"42":"read","43":"read"}}""")
        withHost { port, _ ->
            for ((team, eventFiles, personFiles) in listOf(
                Triple("team_42", listOf(1L, 2L), listOf(7L)),
                Triple("team_43", listOf(3L), listOf(8L)),
            )) {
                val scan = json(get(port, path(team, "/tables/events/scan"), both))
                assertThat(ids(scan)).describedAs(team).containsExactlyElementsOf(eventFiles)
                assertThat(teams(scan)).describedAs(team).containsExactly(team.removePrefix("team_").toLong())
                assertThat(ids(json(get(port, path(team, "/tables/persons/scan"), both))))
                    .describedAs(team).containsExactlyElementsOf(personFiles)
            }
        }
    }

    @Test
    fun `a wildcard token of the org reads one team's stream rows in each team's schema`() {
        withHost { port, _ ->
            for ((team, files) in listOf(
                "team_42" to listOf(1L, 2L),
                "team_43" to listOf(3L),
                "team_48" to listOf(15L),
            )) {
                val scan = json(get(port, path(team, "/tables/events/scan"), star()))
                assertThat(ids(scan)).describedAs(team).containsExactlyElementsOf(files)
            }
        }
    }

    @Test
    fun `a team whose marked namespace has no import table lists and scans the stream tables`() {
        val token = scoped(prd = """{"team_id":{"48":"read"}}""")
        withHost { port, _ ->
            assertThat(json(get(port, path("team_48"), token))["name"].asText()).isEqualTo("team_48")
            assertThat(names(json(get(port, path("team_48", "/tables"), token)))).containsExactly("events", "persons")
            assertThat(ids(json(get(port, path("team_48", "/tables/events/scan"), token)))).containsExactly(15L)
        }
    }

    @Test
    fun `a token whose rtg names a backend this server does not list reads a stream table`() {
        withHost { port, _ ->
            for (path in four) {
                assertThat(get(port, path, scoped(rtg = unlisted)).statusCode()).describedAs(path).isEqualTo(200)
            }
            assertThat(ids(json(get(port, "$ns/tables/events/scan", scoped(rtg = unlisted))))).containsExactly(1L, 2L)
        }
    }

    @Test
    fun `column statistics are read for the scoped files only, and split offsets ride the scan`() {
        withHost { port, _ ->
            lateinit var scan: JsonNode
            val both = "$ns/tables/events/scan?include=column_stats,split_offsets"
            val sent = captured { scan = json(get(port, both, scoped())) }
            assertThat(ids(scan)).containsExactly(1L, 2L)
            scan.forEach { assertThat(it["data_file"]["column_stats"]).describedAs(it.toString()).hasSize(1) }
            val first = scan.single { it["data_file"]["data_file_id"].asLong() == 1L }
            assertThat(first["data_file"]["split_offsets"].map { it.asLong() }).containsExactly(4L, 100L)
            // One statistics statement, entered from the plan's own file ids: never the whole table's.
            assertThat(statistics(sent)).containsExactly(FileRepo.scopedColumnStatsSql(filtered = false).trim())
            val narrowed = "$ns/tables/events/scan?include=column_stats&stats_fields=$teamFieldId"
            assertThat(statistics(captured { json(get(port, narrowed, scoped())) }))
                .containsExactly(FileRepo.scopedColumnStatsSql(filtered = true).trim())
            assertThat(captured { json(get(port, "$ns/tables/events/scan", scoped())) })
                .noneMatch { "hog_file_column_stats" in it }
        }
    }

    @Test
    fun `files whose stats are not proven, or that are quarantined, are never returned`() {
        withHost { port, _ ->
            for (token in listOf(scoped(), star())) {
                val scan = json(get(port, "$ns/tables/events/scan", token))
                assertThat(ids(scan)).doesNotContain(5L, 6L).containsExactly(1L, 2L)
            }
        }
    }

    // ---- refused -------------------------------------------------------------

    @Test
    fun `every caller the rule refuses gets on a stream table the answer of an unserved name, and nothing is read`() {
        // Callers the rule enters and refuses: the exact 404 of a missing namespace.
        val ruled =
            linkedMapOf(
                "no team family: no remote_schemas" to scoped(schemas = null),
                "no team family: another family" to scoped(schemas = """{"analytics":["read"]}"""),
                "prd without the team" to scoped(prd = """{"team_id":{"43":"read"}}"""),
            )
        // Callers the rule does not enter: the answer for any catalog this server does not hold.
        val outside =
            linkedMapOf<String, String?>(
                "cat not naming the path catalog" to scoped(catalog = "other"),
                // A scoped `cat` entry cannot be empty: the token does not verify, here as on any path.
                "cat without read on the path catalog" to scoped(cat = """{"$cust":[]}"""),
                "a tenant" to tenant(),
                "an operator" to operator(),
                "no token" to null,
            )
        for (mode in listOf("enforce", "warn")) {
            withHost(authMode = mode) { port, _ ->
                assertNothingRead(mode) {
                    for ((what, token) in ruled) {
                        for (path in four) assertMissing(get(port, path, token), "$mode, $what: $path")
                    }
                    for ((what, token) in outside) {
                        for (path in four) {
                            val served = get(port, path, token)
                            val unserved = get(port, path.replace("team_42", "team_99"), token)
                            assertThat(served.statusCode()).describedAs("%s, %s: %s", mode, what, path).isIn(401, 404)
                            assertThat(served.statusCode()).isEqualTo(unserved.statusCode())
                            assertThat(served.body()).describedAs("%s, %s: %s", mode, what, path)
                                .isEqualTo(unserved.body().replace("team_99", "team_42"))
                                .doesNotContain("f1.parquet", "a.parquet", orgA)
                        }
                    }
                }
                // A tenant and an operator are never entered in the rule: a missing catalog, not a namespace.
                for (token in listOf(tenant(), operator())) {
                    for (path in four) {
                        val response = get(port, path, token)
                        assertThat(response.statusCode()).describedAs(path).isEqualTo(404)
                        assertThat(mapper.readTree(response.body())["detail"].asText())
                            .describedAs("%s: %s", mode, path).startsWith("catalog '$cust'")
                    }
                }
                // The admitted reader on the same server.
                assertThat(ids(json(get(port, "$ns/tables/events/scan", scoped())))).containsExactly(1L, 2L)
            }
        }
    }

    @Test
    fun `a wildcard token of org A gets a missing namespace for org B's team, stream tables included`() {
        withHost { port, _ ->
            assertNothingRead("org A wildcard on team_77") {
                for (path in relayed("team_77") + path("team_77", "/tables/persons/scan")) {
                    assertMissing(get(port, path, star()), path, "team_77")
                }
            }
            // Org B's own reader is served the team, with its rows only.
            assertThat(ids(json(get(port, path("team_77", "/tables/events/scan"), star(orgB))))).containsExactly(4L)
        }
    }

    @Test
    fun `an org B token whose prd names team 42 gets a missing namespace`() {
        withHost { port, _ ->
            assertNothingRead("org B on team_42") {
                for (token in listOf(scoped(org = orgB), star(orgB))) {
                    for (path in four + "$ns/tables/persons/scan") assertMissing(get(port, path, token), path)
                }
            }
        }
    }

    @Test
    fun `a team with no stored namespace is not served, stream tables included`() {
        withHost { port, _ ->
            assertNothingRead("team_60") {
                for (token in listOf(scoped(prd = """{"team_id":{"60":"read"}}"""), star())) {
                    for (path in relayed("team_60")) assertMissing(get(port, path, token), path, "team_60")
                }
            }
        }
    }

    @Test
    fun `a namespace marked for another value or another field is not served, stream tables included`() {
        val cases =
            mapOf(
                "team_44" to
                    listOf(
                        scoped(prd = """{"team_id":{"44":"read"}}"""),
                        scoped(prd = """{"team_id":{"99":"read"}}""", schemas = """{"team_44":["read"]}"""),
                        star(),
                    ),
                "team_45" to listOf(scoped(prd = """{"team_id":{"45":"read"}}"""), star()),
            )
        withHost { port, _ ->
            assertNothingRead("wrong marks") {
                for ((team, tokens) in cases) {
                    for (token in tokens) {
                        for (path in relayed(team)) assertMissing(get(port, path, token), path, team)
                    }
                }
            }
        }
    }

    @Test
    fun `a name that is not a team's canonical one serves nothing of the stream`() {
        withHost { port, _ ->
            assertNothingRead("non-canonical names") {
                for (name in listOf("team_042", "TEAM_42", "Team_42")) {
                    for (token in listOf(scoped(), star())) {
                        for (path in relayed(name)) {
                            val response = get(port, path, token)
                            assertThat(response.statusCode()).describedAs(path).isEqualTo(404)
                            assertThat(response.body()).doesNotContain("parquet", orgA)
                        }
                    }
                }
            }
        }
    }

    // ---- the published list --------------------------------------------------

    @Test
    fun `an unpublished table of the stream namespace is neither listed nor readable`() {
        withHost { port, _ ->
            assertThat(names(json(get(port, "$ns/tables", star())))).doesNotContain("events_raw")
            for (suffix in listOf("", "/scan")) {
                assertMissingTable(get(port, "$ns/tables/events_raw$suffix", star()), "events_raw")
            }
        }
    }

    @Test
    fun `a table of another namespace of the stream catalog is never reached, under a published name either`() {
        // `other.sessions` is laid out on team_id and holds team 42's file; `sessions` is published and has no
        // table in the stream namespace. `other.secret` is not published.
        withHost { port, _ ->
            assertThat(names(json(get(port, "$ns/tables", star())))).doesNotContain("sessions", "secret")
            for (table in listOf("sessions", "secret")) {
                for (suffix in listOf(
                    "",
                    "/scan",
                )) assertMissingTable(get(port, "$ns/tables/$table$suffix", star()), table)
            }
        }
    }

    @Test
    fun `a published name with no table is neither listed nor readable`() {
        withHost { port, _ ->
            assertThat(names(json(get(port, "$ns/tables", star())))).doesNotContain("gone")
            for (suffix in listOf("", "/scan")) assertMissingTable(get(port, "$ns/tables/gone$suffix", star()), "gone")
        }
    }

    @Test
    fun `a published table without the team_id object-prefix layout is neither listed nor readable`() {
        withHost { port, _ ->
            assertThat(names(json(get(port, "$ns/tables", star())))).doesNotContain("by_day", "no_layout")
            for (table in listOf("by_day", "no_layout")) {
                for (suffix in listOf(
                    "",
                    "/scan",
                )) assertMissingTable(get(port, "$ns/tables/$table$suffix", star()), table)
            }
        }
    }

    @Test
    fun `a published table whose layout and specs disagree is left out of the listing and counted`() {
        withHost(fresh = true) { port, _ ->
            val listing = get(port, "$ns/tables", scoped())
            assertThat(names(json(listing))).doesNotContain("broken").contains("events", "persons")
            val counted =
                get(port, "/metrics", operator()).body().lines()
                    .single { it.startsWith("hoglake_served_stream_unscopable_tables_total{") }
            assertThat(counted).contains("""family="team_*"""")
            assertThat(counted.substringAfterLast(' ').toDouble()).isEqualTo(1.0)
            // Its scan still refuses, in words that name nothing.
            val scan = get(port, "$ns/tables/broken/scan", scoped())
            assertThat(scan.statusCode()).isEqualTo(500)
            assertThat(scan.body()).doesNotContain(streamCatalog, streamNamespace, "broken")
        }
    }

    // ---- collision -----------------------------------------------------------

    @Test
    fun `an import named as a stream table is hidden by it, in any ASCII case`() {
        val token = scoped(prd = """{"team_id":{"43":"read"}}""")
        withHost { port, _ ->
            val listing = json(get(port, path("team_43", "/tables"), token))
            assertThat(names(listing)).containsExactly("events", "pageviews", "persons")
            assertThat(listing.single { it["name"].asText() == "events" }["table_uuid"].asText())
                .isEqualTo(events.tableUuid.toString())
            val table = json(get(port, path("team_43", "/tables/events"), token))
            assertThat(table["table_uuid"].asText()).isEqualTo(events.tableUuid.toString())
            assertThat(table["namespace"].asText()).isEqualTo("team_43")
            val scan = get(port, path("team_43", "/tables/events/scan"), token)
            assertThat(ids(json(scan))).containsExactly(3L)
            assertThat(scan.body()).doesNotContain(shadowedFile, "/$imports/")
            for (variant in listOf("EVENTS", "Events")) {
                for (suffix in listOf("", "/scan")) {
                    assertMissingTable(get(port, path("team_43", "/tables/$variant$suffix"), token), variant, "team_43")
                }
            }
        }
    }

    // ---- leaks ---------------------------------------------------------------

    @Test
    fun `totals are the team's own in the listing and absent from the stream table`() {
        withHost { port, _ ->
            val listing = json(get(port, "$ns/tables", star()))
            val summary = listing.single { it["name"].asText() == "events" }
            // Files 1 and 2: not 3, 4, 15 (other teams), nor 5 and 6 (out of scope).
            assertThat(summary["file_count"].asLong()).isEqualTo(2)
            assertThat(summary["record_count"].asLong()).isEqualTo(300)
            assertThat(summary["file_size_bytes"].asLong()).isEqualTo(30)
            assertThat(listing.single { it["name"].asText() == "persons" }["record_count"].asLong()).isEqualTo(700)
            // A time-travel read would aggregate the whole table, every team's: never asked for.
            val head = streamHead()
            for (query in listOf("", "?totals=true", "?snapshot=$head", "?snapshot=$head&totals=true")) {
                val table = json(get(port, "$ns/tables/events$query", star()))
                for (field in listOf("record_count", "file_count", "file_size_bytes")) {
                    assertThat(table.has(field)).describedAs("%s %s", query, field).isFalse()
                }
            }
        }
    }

    @Test
    fun `no answer names the stream catalog, the stream namespace or the imports catalog outside file paths`() {
        val head = streamHead()
        val token = scoped()
        withHost { port, _ ->
            val paths =
                four +
                    listOf(
                        "$ns/tables/persons",
                        "$ns/tables/persons/scan?include=column_stats",
                        "$ns/tables/pageviews",
                        "$ns/tables/pageviews/scan",
                        "$ns/tables/events_raw",
                        "$ns/tables/EVENTS/scan",
                        "$ns/tables/gone/scan",
                        "$ns/tables/by_day",
                        "$ns/tables/events?snapshot=${head + 1000}",
                        "$ns/tables/events/scan?snapshot=${head + 1000}",
                        "$ns/tables/events?snapshot=0",
                        "$ns/tables/events/scan?at_timestamp=2000-01-01T00:00:00Z",
                        "$ns/tables?at_timestamp=2000-01-01T00:00:00Z",
                        "$ns/tables?snapshot=${importsHead() + 1000}",
                        "$ns/tables/events/scan?snapshot=1&at_timestamp=2000-01-01T00:00:00Z",
                    )
            for (path in paths) {
                val body = get(port, path, token).body().replace(Regex("\"path\":\"[^\"]*\""), "\"path\":\"\"")
                assertThat(body).describedAs(path).doesNotContain(streamCatalog, streamNamespace, imports)
            }
        }
    }

    @Test
    fun `a missing stream catalog is a 500 that names nothing, and the imports are still served`() {
        withHost(stream = "nowhere-dev") { port, _ ->
            for (path in listOf("$ns/tables", "$ns/tables/events", "$ns/tables/events/scan")) {
                val response = get(port, path, scoped())
                assertThat(response.statusCode()).describedAs(path).isEqualTo(500)
                assertThat(mapper.readTree(response.body()))
                    .isEqualTo(mapper.readTree("""{"error":"internal_error","detail":"internal server error"}"""))
            }
            assertThat(get(port, ns, scoped()).statusCode()).isEqualTo(200)
            assertThat(get(port, "$ns/tables/pageviews/scan", scoped()).body()).contains(importFile)
        }
    }

    // ---- snapshots -----------------------------------------------------------

    @Test
    fun `snapshot on a stream table is the stream catalog's, refused in the path catalog's words`() {
        val head = streamHead()
        val importsAt = importsHead()
        assertThat(importsAt).describedAs("the fixture needs two different heads").isGreaterThan(head)
        withHost { port, _ ->
            assertThat(json(get(port, "$ns/tables/events", scoped()))["read_snapshot_id"].asLong()).isEqualTo(head)
            assertThat(json(get(port, "$ns/tables/events?snapshot=$head", scoped()))["read_snapshot_id"].asLong())
                .isEqualTo(head)
            assertThat(ids(json(get(port, "$ns/tables/events/scan?snapshot=$head", scoped())))).containsExactly(1L, 2L)
            // The import catalog's head is beyond the stream catalog's: out of range for a stream table.
            for (suffix in listOf("", "/scan")) {
                val beyond = get(port, "$ns/tables/events$suffix?snapshot=$importsAt", scoped())
                assertThat(beyond.statusCode()).describedAs(beyond.body()).isEqualTo(422)
                assertThat(beyond.body()).contains("out of range for catalog '$cust'").doesNotContain("[0,")
            }
            // ...and the import's own pin.
            val pinned = json(get(port, "$ns/tables/pageviews?snapshot=$importsAt", scoped()))
            assertThat(pinned["read_snapshot_id"].asLong()).isEqualTo(importsAt)
            val floor = db.jdbi.withHandle<Long, Exception> { it.earliestOf(streamCatalog) }
            db.jdbi.useHandle<Exception> { it.setEarliest(streamCatalog, head) }
            try {
                for (suffix in listOf("", "/scan")) {
                    val expired = get(port, "$ns/tables/events$suffix?snapshot=${head - 1}", scoped())
                    assertThat(expired.statusCode()).describedAs(expired.body()).isEqualTo(410)
                    assertThat(expired.body()).contains("below the expiry floor of catalog '$cust'")
                        .doesNotContain("earliest retained")
                }
            } finally {
                db.jdbi.useHandle<Exception> { it.setEarliest(streamCatalog, floor) }
            }
            val early = get(port, "$ns/tables/events?at_timestamp=2000-01-01T00:00:00Z", scoped())
            assertThat(early.statusCode()).describedAs(early.body()).isEqualTo(410)
        }
    }

    private fun Handle.earliestOf(catalog: String): Long =
        createQuery("SELECT earliest_snapshot_id FROM hog_catalog WHERE name = :n").bind("n", catalog)
            .mapTo(Long::class.java).one()

    private fun Handle.setEarliest(
        catalog: String,
        id: Long,
    ) {
        execute("UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE name = ?", id, catalog)
    }

    @Test
    fun `at_timestamp on the listing applies to both catalogs, and snapshot to the imports alone`() {
        withHost { port, _ ->
            // Before the stream namespace and `invoices` existed: the one import of the time.
            assertThat(names(json(get(port, "$ns/tables?at_timestamp=$beforeStream", scoped()))))
                .containsExactly("pageviews")
            // An import catalog's snapshot from that time: the imports then, the stream tables at their head.
            assertThat(names(json(get(port, "$ns/tables?snapshot=$importsBeforeInvoices", scoped()))))
                .containsExactly("events", "pageviews", "persons")
            assertThat(names(json(get(port, "$ns/tables?at_timestamp=${Instant.now().plusSeconds(3600)}", scoped()))))
                .containsExactly("events", "invoices", "pageviews", "persons")
        }
    }

    @Test
    fun `a stream read at an older snapshot or time has that time's files of the one team, and refuses alike`() {
        val team43 = scoped(prd = """{"team_id":{"43":"read"}}""")
        val refused = linkedMapOf("org B" to scoped(org = orgB), "org B wildcard" to star(orgB), "prd of 43" to team43)
        withHost(namespace = "history") { port, _ ->
            val table = "$ns/tables/events"
            val scan = "$table/scan"
            // At head: file 20 and the later 21. File 22 was removed, and 23 is team 43's.
            assertThat(ids(json(get(port, scan, scoped())))).containsExactly(20L, 21L)
            assertThat(ids(json(get(port, path("team_43", "/tables/events/scan"), team43)))).containsExactly(23L)
            for (pin in listOf("snapshot=$historyEarly", "at_timestamp=$historyEarlyAt")) {
                for (token in listOf(scoped(), star())) {
                    val files = json(get(port, "$scan?$pin", token))
                    assertThat(ids(files)).describedAs(pin).containsExactly(20L, 22L)
                    assertThat(teams(files)).describedAs(pin).containsExactly(42L)
                }
                val pinned = json(get(port, "$table?$pin", scoped()))
                assertThat(pinned["read_snapshot_id"].asLong()).describedAs(pin).isEqualTo(historyEarly)
                assertNothingRead(pin) {
                    for ((what, token) in refused) {
                        for (target in listOf(table, scan)) {
                            assertMissing(get(port, "$target?$pin", token), "$what, $pin: $target")
                        }
                    }
                }
            }

            // The listing's totals are the team's files at the time asked for: 20 and 22 then, 20 and 21 now.
            fun records(query: String) =
                json(get(port, "$ns/tables$query", scoped())).single { it["name"].asText() == "events" }["record_count"]
                    .asLong()
            assertThat(records("?at_timestamp=$historyEarlyAt")).isEqualTo(2000 + 2200)
            assertThat(records("")).isEqualTo(2000 + 2100)
        }
    }

    @Test
    fun `the stream table route checks the table and reads its definition at one snapshot`() {
        withHost { port, _ ->
            val before = streamHead()
            val moved = AtomicBoolean(false)
            // The head moves after the check has resolved its snapshot and before the definition is read.
            beforeStatement = { sql ->
                if ("tv.namespace_id = :namespaceId AND tv.name = :name" in sql && moved.compareAndSet(false, true)) {
                    catalogs.createNamespace(streamCatalog, "moved_on")
                }
            }
            val table =
                try {
                    json(get(port, "$ns/tables/events", scoped()))
                } finally {
                    beforeStatement = null
                }
            assertThat(moved.get()).isTrue()
            assertThat(streamHead()).isEqualTo(before + 1)
            assertThat(table["read_snapshot_id"].asLong()).isEqualTo(before)
            assertThat(table["table_uuid"].asText()).isEqualTo(events.tableUuid.toString())
        }
    }

    @Test
    fun `the shared stream table's properties are never served`() {
        withHost { port, _ ->
            val own =
                json(get(port, "/v1/catalogs/$streamCatalog/namespaces/$streamNamespace/tables/events", operator()))
            assertThat(own["properties"]["posthog.pipeline"].asText()).isEqualTo("internal-topic")
            val queries = listOf("", "?totals=true", "?snapshot=${streamHead()}", "?at_timestamp=${Instant.now()}")
            for (query in queries) {
                val served = get(port, "$ns/tables/events$query", scoped())
                assertThat(json(served).path("properties").size()).describedAs(query).isEqualTo(0)
                assertThat(served.body()).describedAs(query).doesNotContain("posthog.pipeline", "internal-topic")
            }
            for (path in listOf("$ns/tables", "$ns/tables/events/scan")) {
                assertThat(get(port, path, scoped()).body()).describedAs(path).doesNotContain("internal-topic")
            }
        }
    }

    // ---- limits --------------------------------------------------------------

    @Test
    fun `a stream scan keeps the file budget, the deadline and the shared cap, and the listing takes one permit`() {
        withHost(fileBudget = 1) { port, _ ->
            val over = get(port, "$ns/tables/events/scan", scoped())
            assertThat(over.statusCode()).describedAs(over.body()).isEqualTo(422)
            assertThat(code(over)).isEqualTo("mount_scan_too_many_files")
            assertThat(ids(json(get(port, "$ns/tables/persons/scan", scoped())))).containsExactly(7L)
        }
        withHost(deadlineMs = 300) { port, _ ->
            // The scan's statement waits on a lock another session holds, past the deadline.
            db.jdbi.useTransaction<Exception> { h ->
                h.execute("LOCK TABLE hog_data_file IN ACCESS EXCLUSIVE MODE")
                val late = get(port, "$ns/tables/events/scan", scoped())
                assertThat(late.statusCode()).describedAs(late.body()).isEqualTo(503)
                assertThat(code(late)).isEqualTo("mount_scan_timeout")
            }
            assertThat(get(port, "$ns/tables/events/scan", scoped()).statusCode()).isEqualTo(200)
        }
        withHost(maxConcurrent = 1, fresh = true) { port, app ->
            val cap = app.mountReadServiceForTest.scanCap!!
            cap.acquire()
            try {
                for (path in listOf("$ns/tables/events/scan", "$ns/tables")) {
                    val busy = get(port, path, scoped())
                    assertThat(busy.statusCode()).describedAs(path).isEqualTo(503)
                    assertThat(code(busy)).describedAs(path).isEqualTo("mount_scan_busy")
                    assertThat(busy.headers().firstValue("Retry-After")).isPresent()
                }
                // The import half takes no permit, and a refused caller is refused before the cap is looked at.
                assertThat(get(port, "$ns/tables/pageviews/scan", scoped()).statusCode()).isEqualTo(200)
                assertMissing(get(port, "$ns/tables", scoped(org = orgB)), "cap full, refused caller")
            } finally {
                cap.release()
            }
            repeat(3) {
                assertThat(get(port, "$ns/tables", scoped()).statusCode()).isEqualTo(200)
                assertThat(get(port, "$ns/tables/events/scan", scoped()).statusCode()).isEqualTo(200)
                assertThat(get(port, "$ns/tables/gone/scan", scoped()).statusCode()).isEqualTo(404)
            }
            assertThat(cap.availablePermits()).isEqualTo(1)
        }
    }

    // ---- unchanged -----------------------------------------------------------

    @Test
    fun `writes and unrelayed reads on a stream table's path are refused and write nothing`() {
        val streamBefore = streamHead()
        val importsBefore = importsHead()
        val writer = scoped(prd = """{"team_id":{"42":"write"}}""", scope = "write")
        withHost { port, _ ->
            val made =
                send(port, "POST", "$ns/tables", writer, """{"name":"events","columns":[{"name":"v","type":"long"}]}""")
            assertThat(made.statusCode()).describedAs(made.body()).isEqualTo(403)
            assertThat(code(made)).isEqualTo("read_only_catalog")
            val dropped = send(port, "DELETE", "$ns/tables/events", writer)
            assertThat(dropped.statusCode()).describedAs(dropped.body()).isEqualTo(403)
            assertThat(code(dropped)).isEqualTo("read_only_catalog")
            val files = get(port, "$ns/tables/events/files", scoped())
            assertThat(files.statusCode()).describedAs(files.body()).isEqualTo(501)
            assertThat(code(files)).isEqualTo("mount_operation_unavailable")
        }
        assertThat(streamHead()).isEqualTo(streamBefore)
        assertThat(importsHead()).isEqualTo(importsBefore)
    }

    @Test
    fun `without the stream block the server answers as before`() {
        val token = scoped()
        val unchanged = listOf(ns, "$ns/tables/pageviews", "$ns/tables/pageviews/scan", "$ns/tables/nope")
        val withStream = withHost { port, _ -> unchanged.map { get(port, it, token).body() } }
        withHost(stream = null) { port, _ ->
            assertThat(names(json(get(port, "$ns/tables", token)))).containsExactly("invoices", "pageviews")
            for (suffix in listOf(
                "",
                "/scan",
            )) assertMissingTable(get(port, "$ns/tables/events$suffix", token), "events")
            // team_43's own `events` import is served again.
            val imported =
                get(port, path("team_43", "/tables/events/scan"), scoped(prd = """{"team_id":{"43":"read"}}"""))
            assertThat(imported.body()).contains(shadowedFile)
            assertThat(unchanged.map { get(port, it, token).body() }).isEqualTo(withStream)
        }
    }

    @Test
    fun `the legacy mount reads as before on a server with the stream block`() {
        val reader =
            TestTokens.mint(
                pair,
                TestTokens.scopedClaims(
                    prd = """{"team_id":{"42":"read","43":"read"}}""",
                    catalogs = """{"$mount":["read"]}""",
                    iat = System.currentTimeMillis() / 1000 - 5,
                    org = orgA,
                ),
            )
        withHost { port, _ ->
            val base = "/v1/catalogs/$mount/namespaces/$streamNamespace/tables"
            val tables = json(get(port, base, reader))
            assertThat(names(tables)).containsExactly("events")
            assertThat(tables.single()["file_count"].asLong()).isEqualTo(3)
            val scan = json(get(port, "$base/events/scan?include=column_stats", reader))
            assertThat(ids(scan)).containsExactly(1L, 2L, 3L)
            assertThat(teams(scan)).containsExactly(42L, 43L)
            assertThat(scan.map { it["data_file"]["column_stats"].size() }).containsExactly(1, 1, 1)
            assertThat(get(port, "$base/events_raw", reader).statusCode()).isEqualTo(404)
        }
    }

    @Test
    fun `an operator reading the stream catalog under its own name keeps the local path`() {
        withHost { port, _ ->
            val base = "/v1/catalogs/$streamCatalog/namespaces/$streamNamespace"
            assertThat(names(json(get(port, "$base/tables", operator()))))
                .contains("events", "events_raw", "persons", "broken")
            val table = json(get(port, "$base/tables/events", operator()))
            assertThat(table["namespace"].asText()).isEqualTo(streamNamespace)
        }
    }

    @Test
    fun `stream series are labelled by the family and never by a team, an org or a table`() {
        withHost(maxConcurrent = 1, fresh = true) { port, app ->
            json(get(port, "$ns/tables", scoped()))
            json(get(port, "$ns/tables/events/scan", scoped()))
            app.mountReadServiceForTest.scanCap!!.acquire()
            try {
                assertThat(get(port, "$ns/tables/events/scan", scoped()).statusCode()).isEqualTo(503)
            } finally {
                app.mountReadServiceForTest.scanCap!!.release()
            }
            val series =
                get(port, "/metrics", operator()).body().lines()
                    .filter { it.startsWith("hoglake_mount_scans") || it.startsWith("hoglake_mount_refusals_total") } +
                    get(port, "/metrics", operator()).body().lines()
                        .filter { it.startsWith("hoglake_served_stream_unscopable_tables_total{") }
            val labels =
                series.flatMap {
                    Regex("""(catalog|family)="([^"]*)"""").findAll(it).map {
                            m ->
                        m.groupValues[2]
                    }
                }
            assertThat(labels).isNotEmpty().allMatch { it == "team_*" }
            assertThat(series.joinToString("\n")).contains("mount_scan_busy")
                .doesNotContain(orgA, streamCatalog, streamNamespace, "\"events\"", "team_42")
        }
    }
}
