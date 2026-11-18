package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.SecretString
import com.posthog.hoglake.auth.LocalSigningKey
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
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
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

/**
 * What a reader of a remote schema gets on the wire (docs/auth-tenancy.md
 * §6.5c and §6.5d), on two real servers in one JVM that trust one local
 * issuer. The Customer holds the catalog `lake` and relays the `team` family
 * to the Host. The Host serves `team_42` from two catalogs: the import
 * `events` from `imports` and the stream table `persons` from `millrace`.
 * The three catalogs' heads differ on purpose, so each snapshot id names one
 * catalog: a remote table's pin is its own catalog's, and the Customer's own
 * snapshot id is not a pin. Reader stubs copy the bodies asserted here.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteTablePinIntegrationTest {
    private val hostDb = PgTestSupport.freshDatabase()
    private val customerDb = PgTestSupport.freshDatabase()
    private val hostCatalogs = CatalogService(hostDb.jdbi)
    private val customerCatalogs = CatalogService(customerDb.jdbi)
    private val key = LocalSigningKey.generate()
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val org = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val imports = "imports"
    private val stream = "millrace"
    private val lake = "lake"
    private val ns = "/v1/catalogs/$lake/namespaces/team_42"
    private val events: TableInfo
    private val persons: TableInfo
    private val firstFile = "s3://bucket/$imports/org_id=$org/team_id=42/events/a.parquet"

    /** The imports snapshot that holds [firstFile] alone, and an instant after it and before the next commit. */
    private val firstPin: Long
    private val between: Instant
    private val host: Running
    private val customer: Running

    init {
        // The Customer's head ends far above both Host heads, so none of its ids is one of theirs.
        customerCatalogs.createCatalog(lake, "s3://bucket/$lake")
        repeat(24) { customerCatalogs.createNamespace(lake, "pad_$it") }

        hostCatalogs.createCatalog(imports, "s3://bucket/$imports")
        hostCatalogs.createNamespace(imports, "team_42")
        hostCatalogs.setNamespaceContainment(imports, "team_42", NamespaceContainment("team_id", 42, org))
        events = hostCatalogs.createTable(imports, "team_42", "events", listOf(ColumnDef("v", ColType.LONG)))
        firstPin = commit(firstFile)
        Thread.sleep(50)
        between = Instant.now()
        Thread.sleep(50)
        commit(firstFile.replace("a.parquet", "b.parquet"))

        // The stream table, laid out on team_id with one file of team 42, as ServedStreamIntegrationTest lays it out.
        hostCatalogs.createCatalog(stream, "s3://bucket/millrace/$stream")
        hostCatalogs.createNamespace(stream, "ingest")
        persons = hostCatalogs.createTable(stream, "ingest", "persons", listOf(ColumnDef("team_id", ColType.LONG)))
        val field = persons.columns.single().fieldId
        hostDb.jdbi.useHandle<Exception> { h ->
            val catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n").bind("n", stream)
                    .mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) VALUES (?, ?, 1, 0)",
                catalogId,
                persons.tableId,
            )
            h.execute(
                "INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, " +
                    "transform) VALUES (?, ?, 1, 0, ?, 'identity')",
                catalogId,
                persons.tableId,
                field,
            )
            h.execute(
                "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                    "VALUES (?, ?, 1, ?)",
                catalogId,
                persons.tableId,
                field,
            )
            h.execute(
                "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path, " +
                    "record_count, file_size_bytes, row_id_start, spec_id, stats_state, prefix_quarantined) " +
                    "VALUES (?, 501, ?, 1, NULL, ?, 10, 100, 0, 1, 'provided', false)",
                catalogId,
                persons.tableId,
                "s3://bucket/millrace/$stream/data/ingest/persons/team_id=42/f501.parquet",
            )
            h.execute(
                "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) " +
                    "VALUES (?, 501, 0, '42')",
                catalogId,
            )
        }

        val auth =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = "local",
                authSigningKey = SecretString(key.encode()),
                authIssuer = TestTokens.ISSUER,
                authPredicateFields = "team_id",
                authSessionProfile = "on",
            )
        val family =
            """{"family":"team","source_catalog":"$imports","predicate_field":"team_id",""" +
                """"stream":{"catalog":"$stream","namespace":"ingest","tables":["persons"]}}"""
        host = start(App.build(auth.copy(servedNamespacesFile = file("""{"families":[$family]}""")), hostDb.jdbi))
        val hosts =
            """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e",""" +
                """"match":{"type":"namespace_family","family":"team"},"kind":"hoglake",""" +
                """"url":"http://127.0.0.1:${host.port}","auth":"forward","allow_insecure_http":true}]}"""
        customer = start(App.build(auth.copy(mountHostsFile = file(hosts)), customerDb.jdbi))
    }

    @AfterAll
    fun close() {
        customer.close()
        host.close()
        http.close()
        hostDb.close()
        customerDb.close()
    }

    private class Running(
        val port: Int,
        private val app: App,
        private val server: EmbeddedServer<*, *>,
    ) : AutoCloseable {
        override fun close() {
            server.stop(0, 1000)
            app.close()
        }
    }

    private fun start(app: App): Running {
        val server = embeddedServer(Netty, configure = { configureApiListener(app.cfg) }) { app.module(this) }
        server.start(wait = false)
        return Running(runBlocking { server.engine.resolvedConnectors().first().port }, app, server)
    }

    private fun file(content: String): String =
        Files.createTempFile("hoglake-remote-pin-", ".json").also {
            it.toFile().deleteOnExit()
            Files.writeString(it, content)
        }.toString()

    /** One more file in the Host's `team_42.events`: one more imports snapshot, returned. */
    private fun commit(path: String): Long =
        CommitService(hostDb.jdbi).commit(
            imports,
            CommitRequest(
                appends = listOf(TableAppend("team_42", "events", listOf(FileRegistration(path, 10, 1_000)))),
            ),
        ).snapshotId

    private fun head(
        db: PgTestSupport.TestDb,
        catalog: String,
    ) = CatalogService(db.jdbi).getCatalog(catalog).headSnapshotId

    private fun customerHead() = head(customerDb, lake)

    private fun importsHead() = head(hostDb, imports)

    private fun streamHead() = head(hostDb, stream)

    /** [claims] signed by the local issuer both servers trust. */
    private fun mint(claims: String): String =
        key.sign(mapper.readTree("""{"alg":"ES256","kid":"${key.keyId}","typ":"JWT"}"""), mapper.readTree(claims))

    /** A session-profile token on `lake` with the `team` family entry, as the issuer shapes it. */
    private fun reader(teams: String = """{"42":"read"}"""): String =
        mint(
            TestTokens.scopedClaims(
                prd = """{"team_id":$teams}""",
                catalogs = """{"$lake":["read"]}""",
                iat = System.currentTimeMillis() / 1000 - 5,
                extra = TestTokens.sessionProfile(lake, """{"team":["read"]}"""),
                org = org,
            ),
        )

    private fun star() = reader("""{"*":"read"}""")

    private fun send(
        port: Int,
        method: String,
        path: String,
        token: String,
        body: String? = null,
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .timeout(Duration.ofSeconds(60))
                .header("Authorization", "Bearer $token")
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    /** A 200 from the Customer, as the reader gets it. */
    private fun get(
        path: String,
        token: String = reader(),
    ): String {
        val response = send(customer.port, "GET", path, token)
        assertThat(response.statusCode()).describedAs("%s: %s", path, response.body()).isEqualTo(200)
        return response.body()
    }

    private fun pin(body: String) = mapper.readTree(body)["read_snapshot_id"].asLong()

    private fun paths(scan: String) = mapper.readTree(scan).map { it["data_file"]["path"].asText() }

    private fun keys(body: String) = mapper.readTree(body).fieldNames().asSequence().toList()

    @Test
    fun `the listing names a held team as a remote schema, and a wildcard token's lists no team`() {
        assertThat(get("/v1/catalogs/$lake/namespaces")).isEqualTo("""[{"name":"team_42","remote":true}]""")
        assertThat(get("/v1/catalogs/$lake/namespaces", star())).isEqualTo("[]")
    }

    @Test
    fun `the namespace answer is a name and the flag, for an explicit and a wildcard token`() {
        for (token in listOf(reader(), star())) {
            assertThat(get(ns, token)).isEqualTo("""{"name":"team_42","remote":true}""")
        }
    }

    @Test
    fun `a remote table says remote and carries the head of the catalog that holds it`() {
        val heads = listOf(importsHead(), streamHead(), customerHead())
        assertThat(heads).describedAs("the fixture needs three different heads").doesNotHaveDuplicates()
        val column = """"nullable":true,"field_id":1,"ordinal":0}]"""
        val expected =
            mapOf(
                "events" to
                    """{"name":"events","namespace":"team_42","table_uuid":"${events.tableUuid}",""" +
                    """"columns":[{"name":"v","type":"long",$column,"read_snapshot_id":${heads[0]},""" +
                    """"properties":{},"remote":true}""",
                "persons" to
                    """{"name":"persons","namespace":"team_42","table_uuid":"${persons.tableUuid}",""" +
                    """"columns":[{"name":"team_id","type":"long",$column,"read_snapshot_id":${heads[1]},""" +
                    """"partition_spec":{"spec_id":1,"fields":[{"source_field_id":1,"transform":"identity"}]},""" +
                    """"object_prefix_field":"team_id","properties":{},"remote":true}""",
            )
        for ((table, body) in expected) {
            val relayed = get("$ns/tables/$table")
            assertThat(relayed).describedAs(table).isEqualTo(body)
            // Relayed byte for byte: the Host's own answer.
            assertThat(send(host.port, "GET", "$ns/tables/$table", reader()).body()).isEqualTo(relayed)
        }
    }

    @Test
    fun `a pinned table and its scan stay at the pin after the Host commits again`() {
        val pin = pin(get("$ns/tables/events"))
        val files = paths(get("$ns/tables/events/scan?snapshot=$pin"))
        commit(firstFile.replace("a.parquet", "c-$pin.parquet"))
        assertThat(importsHead()).isGreaterThan(pin)
        assertThat(pin(get("$ns/tables/events?snapshot=$pin"))).isEqualTo(pin)
        assertThat(paths(get("$ns/tables/events/scan?snapshot=$pin"))).isEqualTo(files)
        // At head, the reader would see the new file.
        assertThat(pin(get("$ns/tables/events"))).isEqualTo(importsHead())
        assertThat(paths(get("$ns/tables/events/scan"))).hasSize(files.size + 1).containsAll(files)
    }

    @Test
    fun `at_timestamp on a remote table resolves in the catalog that holds it`() {
        val at = get("$ns/tables/events?at_timestamp=$between")
        assertThat(pin(at)).isEqualTo(firstPin).isNotEqualTo(customerHead())
        assertThat(paths(get("$ns/tables/events/scan?at_timestamp=$between"))).containsExactly(firstFile)
    }

    @Test
    fun `the Customer's own head is not a pin and is refused on a remote table`() {
        val own = customerHead()
        assertThat(own).isGreaterThan(importsHead()).isGreaterThan(streamHead())
        for (table in listOf("events", "persons")) {
            for (suffix in listOf("", "/scan")) {
                val refused = send(customer.port, "GET", "$ns/tables/$table$suffix?snapshot=$own", reader())
                assertThat(refused.statusCode()).describedAs("%s%s: %s", table, suffix, refused.body()).isEqualTo(422)
                assertThat(refused.body()).contains("out of range for catalog '$lake'")
            }
        }
    }

    @Test
    fun `a local table's answers and receipts carry no remote key`() {
        val operator = mint(TestTokens.operatorClaims())
        val base = "/v1/catalogs/$lake/namespaces/local/tables"

        fun post(
            path: String,
            body: String,
        ) = send(customer.port, "POST", path, operator, body)
        val namespace = post("/v1/catalogs/$lake/namespaces", """{"name":"local"}""")
        val created = post(base, """{"name":"t","columns":[{"name":"v","type":"long"}]}""")
        val altered = post("$base/t/alter", """{"ops":[{"op":"set_table_comment","comment":"c"}]}""")
        val read = send(customer.port, "GET", "$base/t", operator)
        assertThat(listOf(namespace, created, altered, read).map { it.statusCode() })
            .containsExactly(201, 201, 200, 200)
        // The key lists of these answers before the flag existed.
        val totals = listOf("record_count", "file_count", "file_size_bytes")
        val identity = listOf("name", "namespace", "table_uuid", "columns", "read_snapshot_id")
        assertThat(keys(created.body())).isEqualTo(identity + totals + listOf("properties", "snapshot_id"))
        assertThat(keys(altered.body())).isEqualTo(identity + totals + listOf("comment", "properties", "snapshot_id"))
        assertThat(keys(read.body())).isEqualTo(identity + listOf("comment", "properties"))
    }
}
