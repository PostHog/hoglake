package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.http.HttpMethod
import io.ktor.server.application.ApplicationCallPipeline
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
import java.util.concurrent.atomic.AtomicInteger

/**
 * The Host/Client split end to end (plan rows 1.3h and 1.3c): two real
 * servers. The Host holds a mount over a source table with several teams'
 * files; the Client holds its own catalog `lake` and a HOGLAKE_MOUNT_HOSTS_FILE.
 * Every request the Host receives is counted, so "refused at the Client"
 * is "the Host saw nothing".
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MountClientIntegrationTest {
    private val hostDb = PgTestSupport.freshDatabase()
    private val clientDb = PgTestSupport.freshDatabase()
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-mount-client-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
    private val otherMount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events"
    private val source = "millrace-dev"
    private val scan = "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan"

    private val sourceTableUuid: java.util.UUID
    private val teamFieldId: Long

    /** The export admits teams 7, 42, 49 and 99; the reader's token names 7 and 42. */
    private val registry =
        MountRegistry {
            if (it == mount) MountRecord(it, revision = 1, enabled = true, values = listOf(7, 42, 49, 99)) else null
        }

    init {
        val catalogs = CatalogService(hostDb.jdbi)
        catalogs.createCatalog(source, "s3://bucket/millrace/$source")
        catalogs.createNamespace(source, "ingest")
        val events = catalogs.createTable(source, "ingest", "events_v2", listOf(ColumnDef("team_id", ColType.LONG)))
        sourceTableUuid = events.tableUuid
        teamFieldId = events.columns.single { it.def.name == "team_id" }.fieldId
        // A catalog on the Host with the name of one on the Client: the Client's listing must not show it.
        catalogs.createCatalog("lake", "s3://bucket/host-lake")
        hostDb.jdbi.useHandle<Exception> { h ->
            val catId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n").bind("n", source)
                    .mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                    "VALUES (?, ?, 1, ?)",
                catId,
                events.tableId,
                teamFieldId,
            )
            h.execute(
                "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) VALUES (?, ?, 1, 0)",
                catId,
                events.tableId,
            )
            h.execute(
                "INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, " +
                    "transform) VALUES (?, ?, 1, 0, ?, 'identity')",
                catId,
                events.tableId,
                teamFieldId,
            )

            fun file(
                id: Long,
                team: String,
            ) {
                h.execute(
                    "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, " +
                        "end_snapshot, path, record_count, file_size_bytes, row_id_start, spec_id, " +
                        "stats_state, prefix_quarantined) VALUES (?, ?, ?, 1, NULL, ?, ?, ?, ?, 1, 'provided', false)",
                    catId,
                    id,
                    events.tableId,
                    "s3://bucket/millrace/$source/data/ingest/events_v2/team_id=$team/f$id.parquet",
                    id * 100,
                    id * 10,
                    id,
                )
                h.execute(
                    "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) " +
                        "VALUES (?, ?, 0, ?)",
                    catId,
                    id,
                    team,
                )
            }
            file(1, "42")
            file(2, "42")
            file(3, "7")
            file(4, "99")
            file(5, "13")
            h.execute("UPDATE hog_mount_switch SET enabled = true")
        }
        CatalogService(clientDb.jdbi).also {
            it.createCatalog("lake", "s3://bucket/client-lake")
            it.createNamespace("lake", "team_1")
        }
    }

    @AfterAll
    fun close() {
        http.close()
        hostDb.close()
        clientDb.close()
        Files.deleteIfExists(keyFile)
    }

    private fun authConfig() =
        Config(port = 0, hydratorIntervalMs = 0).copy(
            authMode = "enforce",
            authIssuer = TestTokens.ISSUER,
            authJwksFile = keyFile.toString(),
            authPredicateFields = "team_id",
        )

    private class Running(
        val port: Int,
        private val app: App,
        private val server: EmbeddedServer<*, *>,
        /** Requests this server received, whatever it answered. */
        val received: AtomicInteger,
    ) : AutoCloseable {
        override fun close() {
            server.stop(0, 1000)
            app.close()
        }
    }

    private fun start(app: App): Running {
        val received = AtomicInteger()
        val server =
            embeddedServer(Netty, configure = { configureApiListener(app.cfg) }) {
                intercept(ApplicationCallPipeline.Setup) { received.incrementAndGet() }
                app.module(this)
            }
        server.start(wait = false)
        return Running(runBlocking { server.engine.resolvedConnectors().first().port }, app, server, received)
    }

    private fun host(): Running {
        val cfg =
            authConfig().copy(
                mountSourceCatalog = source,
                mountSourceTableUuid = sourceTableUuid.toString(),
                mountSourceFieldId = teamFieldId.toString(),
            )
        return start(App.build(cfg, hostDb.jdbi, mountRegistry = registry))
    }

    /** A hosts file sending every mount name to the Host at [hostPort]. */
    private fun hostsFile(hostPort: Int): String =
        Files.createTempFile("hoglake-mount-hosts-", ".json").also {
            it.toFile().deleteOnExit()
            Files.writeString(
                it,
                """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","match":{"type":"events"},""" +
                    """"kind":"hoglake","url":"http://127.0.0.1:$hostPort","auth":"forward"}]}""",
            )
        }.toString()

    /** A PostgreSQL-default Client of the Host at [hostPort]; null is the same server with the setting unset. */
    private fun client(hostPort: Int?): Running =
        start(App.build(authConfig().copy(mountHostsFile = hostPort?.let(::hostsFile) ?: ""), clientDb.jdbi))

    /** A Client with no PostgreSQL at all, as the customer hoglake is. */
    private fun postgresFreeClient(hostPort: Int): Running =
        start(
            App.build(
                authConfig().copy(
                    defaultBackend = "none",
                    jdbcUrl = "jdbc:no-such-driver:must-not-open",
                    dbUser = "",
                    dbPassword = "",
                    mountHostsFile = hostsFile(hostPort),
                ),
            ),
        )

    /** A scoped token for teams 7 and 42 on [catalogs]. */
    private fun reader(
        catalogs: String = """{"$mount":["read"]}""",
        prd: String = """{"team_id":{"7":"read","42":"read"}}""",
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(prd = prd, catalogs = catalogs, iat = iat, exp = iat + 900),
        )
    }

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun tenant(catalog: String) =
        TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(catalog to listOf("admin"))))

    private fun send(
        port: Int,
        method: HttpMethod,
        path: String,
        token: String?,
    ): HttpResponse<String> {
        val body = if (method == HttpMethod.Get || method == HttpMethod.Delete) "" else "{}"
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .header("Content-Type", "application/json")
                .method(method.value, HttpRequest.BodyPublishers.ofString(body))
        token?.let { request.header("Authorization", "Bearer $it") }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString())
    }

    private fun get(
        port: Int,
        path: String,
        token: String?,
    ) = send(port, HttpMethod.Get, path, token)

    private fun error(body: String): String = body.substringAfter("\"error\":\"", "").substringBefore('"')

    /** Every catalog route in the route table, with its parameters filled for [catalog]. */
    private fun catalogRoutes(catalog: String): List<Pair<AuthPolicy.Entry, String>> =
        AuthPolicy.rules.filter { "{catalog}" in it.pattern }.map { rule ->
            rule to
                rule.pattern
                    .replace("{catalog}", catalog)
                    .replace("{namespace}", "ingest")
                    .replace("{table}", "events_v2")
                    .replace("{tableUuid}", "00000000-0000-0000-0000-000000000001")
                    .replace("{operation}", "00000000-0000-0000-0000-000000000002")
                    .replace("{upload}", "00000000-0000-0000-0000-000000000003")
                    .replace(Regex("\\{[a-zA-Z]+}"), "x")
        }

    private val served =
        setOf(
            "/v1/catalogs/{catalog}",
            "/v1/catalogs/{catalog}/namespaces",
            "/v1/catalogs/{catalog}/namespaces/{namespace}",
            "/v1/catalogs/{catalog}/namespaces/{namespace}/tables",
            "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}",
            "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan",
        )

    @Test
    fun `a reader for teams 7 and 42 gets exactly those teams' files through the client`() {
        host().use { host ->
            client(host.port).use { client ->
                val response = get(client.port, scan, reader())
                assertThat(response.statusCode()).isEqualTo(200)
                val files = mapper.readTree(response.body())
                assertThat(files.map { it["data_file"]["data_file_id"].asLong() }).containsExactlyInAnyOrder(1L, 2L, 3L)
                assertThat(files.map { it["team_id"].asLong() }.toSet()).containsExactlyInAnyOrder(7L, 42L)
                files.forEach { f ->
                    // The Host's paths, untouched: the reader goes to the Host's bucket for them.
                    assertThat(f["data_file"]["path"].asText())
                        .startsWith(
                            "s3://bucket/millrace/$source/data/ingest/events_v2/team_id=${f["team_id"].asLong()}/",
                        )
                }
                // A narrower and a wider token change what the Host returns, not what the Client does.
                val only7 = mapper.readTree(get(client.port, scan, reader(prd = """{"team_id":{"7":"read"}}""")).body())
                assertThat(only7.map { it["team_id"].asLong() }).containsExactly(7L)
                val star = mapper.readTree(get(client.port, scan, reader(prd = """{"team_id":{"*":"read"}}""")).body())
                // `*` is the export's values {7,42,49,99}: team 13's file is outside the export.
                assertThat(star.map { it["team_id"].asLong() }.toSet()).containsExactlyInAnyOrder(7L, 42L, 99L)
                val outside = get(client.port, scan, reader(prd = """{"team_id":{"13":"read"}}"""))
                assertThat(mapper.readTree(outside.body())).isEmpty()
            }
        }
    }

    @Test
    fun `every served route answers through the client exactly as the host answers it`() {
        host().use { host ->
            client(host.port).use { client ->
                val token = reader()
                val paths =
                    listOf(
                        "/v1/catalogs/$mount",
                        "/v1/catalogs/$mount/namespaces",
                        "/v1/catalogs/$mount/namespaces/ingest",
                        "/v1/catalogs/$mount/namespaces/ingest/tables",
                        "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2",
                        "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2?totals=true",
                        scan,
                        "$scan?include=column_stats&include=split_offsets",
                        "$scan?snapshot=1",
                        // The Host's own refusals come back as the Host worded them.
                        "$scan?snapshot=999999",
                        "$scan?include=nonsense",
                        "$scan?snapshot=1&at_timestamp=2026-09-05T00:00:00Z",
                        "/v1/catalogs/$mount/namespaces/other",
                        "/v1/catalogs/$mount/namespaces/ingest/tables/other_events",
                        "/v1/catalogs/$mount/namespaces/ingest/tables/other_events/scan",
                    )
                for (path in paths) {
                    val direct = get(host.port, path, token)
                    val proxied = get(client.port, path, token)
                    assertThat(proxied.statusCode()).describedAs(path).isEqualTo(direct.statusCode())
                    assertThat(proxied.body()).describedAs(path).isEqualTo(direct.body())
                }
                val direct = get(host.port, "/v1/catalogs/$mount", token).body()
                assertThat(direct).contains(""""data_path":"s3://bucket/millrace/$source"""", """"read_only":true""")
                // The listing: the Client's own catalog, then the Host's mount entry, and none of the Host's catalogs.
                val both = reader("""{"lake":["read"],"$mount":["read"]}""")
                val listed = get(client.port, "/v1/catalogs", both).body()
                assertThat(
                    listed,
                ).startsWith("""[{"name":"lake","data_path":"s3://bucket/client-lake"""").endsWith(",$direct]")
                assertThat(listed).doesNotContain("host-lake", "\"name\":\"$source\"")
                assertThat(get(client.port, "/v1/catalogs", token).body()).isEqualTo("[$direct]")
            }
        }
    }

    @Test
    fun `a token whose cat does not name the catalog is refused at the client and the host sees nothing`() {
        host().use { host ->
            client(host.port).use { client ->
                val before = host.received.get()
                val tokens =
                    mapOf(
                        "scoped for another catalog" to reader("""{"lake":["read"]}"""),
                        "scoped for another mount" to reader("""{"$otherMount":["read"]}"""),
                        "tenant naming the mount" to tenant(mount),
                        "operator" to operator(),
                        "none" to null,
                    )
                for ((who, token) in tokens) {
                    for ((rule, path) in catalogRoutes(mount)) {
                        val response = send(client.port, rule.method, path, token)
                        assertThat(response.statusCode()).describedAs("%s %s %s", who, rule.method.value, path)
                            .isIn(401, 403, 404)
                    }
                    // Its listing shows no mount and asks the Host nothing.
                    // (A token naming another mount is that mount's reader: its listing is the Host's to answer.)
                    if (token != null && who != "scoped for another mount") {
                        assertThat(get(client.port, "/v1/catalogs", token).body()).doesNotContain(mount)
                    }
                }
                // The plain case, spelled out: 404 worded as a missing catalog.
                val refused = get(client.port, scan, reader("""{"lake":["read"]}"""))
                assertThat(refused.statusCode()).isEqualTo(404)
                assertThat(error(refused.body())).isEqualTo("not_found")
                assertThat(host.received.get()).isEqualTo(before)
            }
        }
    }

    @Test
    fun `every other route on the mount is refused at the client with the code the host answers`() {
        host().use { host ->
            client(host.port).use { client ->
                for (cat in listOf("read", "write")) {
                    val token = reader("""{"$mount":["$cat"]}""")
                    for ((rule, path) in catalogRoutes(mount)) {
                        val direct = send(host.port, rule.method, path, token)
                        val before = host.received.get()
                        val proxied = send(client.port, rule.method, path, token)
                        val what = "${rule.method.value} $path ($cat)"
                        assertThat(proxied.statusCode()).describedAs(what).isEqualTo(direct.statusCode())
                        assertThat(error(proxied.body())).describedAs(what).isEqualTo(error(direct.body()))
                        if (rule.method == HttpMethod.Get && rule.pattern in served) {
                            assertThat(proxied.statusCode()).describedAs(what).isEqualTo(200)
                            assertThat(host.received.get()).describedAs(what).isEqualTo(before + 1)
                        } else {
                            // A write (403 read_only_catalog), an unserved read (501
                            // mount_operation_unavailable) or a route closed to scoped
                            // callers (403 forbidden): answered here, never sent.
                            assertThat(error(proxied.body())).describedAs(what)
                                .isIn("read_only_catalog", "mount_operation_unavailable", "forbidden")
                            assertThat(host.received.get()).describedAs(what).isEqualTo(before)
                        }
                    }
                }
                val write =
                    send(client.port, HttpMethod.Post, "/v1/catalogs/$mount/commit", reader("""{"$mount":["write"]}"""))
                assertThat(write.statusCode() to error(write.body())).isEqualTo(403 to "read_only_catalog")
                val files = get(client.port, "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/files", reader())
                assertThat(files.statusCode() to error(files.body())).isEqualTo(501 to "mount_operation_unavailable")
            }
        }
    }

    /** What the Client answers on its own catalog and listing, for several callers. */
    private fun ownCatalog(port: Int): Map<String, Pair<Int, String>> {
        val callers =
            mapOf(
                "operator" to operator(),
                "tenant" to tenant("lake"),
                "scoped" to reader("""{"lake":["read"]}"""),
            )
        return catalogRoutes("lake")
            .filter { (rule, _) -> rule.method == HttpMethod.Get }
            .map { (_, path) -> path.replace("/namespaces/ingest", "/namespaces/team_1") }
            .plus(listOf("/v1/catalogs", "/v1/catalogs/nope", "/v1/info"))
            .flatMap { path ->
                callers.map { (who, token) ->
                    val response = get(port, path, token)
                    "$who $path" to (response.statusCode() to response.body())
                }
            }.toMap()
    }

    @Test
    fun `a catalog that is not a mount is unaffected, and so is everything when the setting is unset`() {
        host().use { host ->
            val before = host.received.get()
            val withHost =
                client(host.port).use { client ->
                    // A write to the Client's own catalog still lands.
                    val created =
                        http.send(
                            HttpRequest.newBuilder(
                                URI.create("http://127.0.0.1:${client.port}/v1/catalogs/lake/namespaces"),
                            )
                                .header("Content-Type", "application/json")
                                .header("Authorization", "Bearer ${operator()}")
                                .POST(HttpRequest.BodyPublishers.ofString("""{"name":"made_with_host_set"}"""))
                                .build(),
                            HttpResponse.BodyHandlers.ofString(),
                        )
                    assertThat(created.statusCode()).isEqualTo(201)
                    ownCatalog(client.port)
                }
            assertThat(host.received.get()).isEqualTo(before)
            val unset =
                client(null).use { client ->
                    val own = ownCatalog(client.port)
                    // Unset, a mount name is whatever it was before: here, a catalog nobody created.
                    val asBefore = get(client.port, "/v1/catalogs/$mount", reader())
                    assertThat(asBefore.statusCode() to error(asBefore.body())).isEqualTo(404 to "not_found")
                    assertThat(get(client.port, "/v1/catalogs", reader()).body()).isEqualTo("[]")
                    own
                }
            // Same bytes on every read of its own catalog, with the setting set or not.
            assertThat(withHost).isEqualTo(unset)
            assertThat(withHost.values.map { it.first }).contains(200)
            assertThat(host.received.get()).isEqualTo(before)
        }
    }

    @Test
    fun `a host that is down is a typed 503, and the client's own catalog still answers`() {
        val host = host()
        val hostPort = host.port
        client(hostPort).use { client ->
            assertThat(get(client.port, scan, reader()).statusCode()).isEqualTo(200)
            host.close()
            for (path in listOf(scan, "/v1/catalogs/$mount", "/v1/catalogs")) {
                val response = get(client.port, path, reader())
                assertThat(response.statusCode()).describedAs(path).isEqualTo(503)
                assertThat(error(response.body())).describedAs(path).isEqualTo("mount_host_unavailable")
                assertThat(response.headers().firstValue("Retry-After")).isPresent()
                assertThat(response.body()).doesNotContain("127.0.0.1", "$hostPort")
            }
            assertThat(get(client.port, "/v1/catalogs/lake", operator()).statusCode()).isEqualTo(200)
            assertThat(get(client.port, "/v1/catalogs", operator()).statusCode()).isEqualTo(200)
            assertThat(get(client.port, "/livez", null).statusCode()).isEqualTo(200)
            // Counted per mount on the Client's metrics, by the opaque name only.
            val metrics = get(client.port, "/metrics", operator()).body().lines()
            val errors = metrics.filter { it.startsWith("hoglake_mount_proxy_upstream_errors_total{") }
            assertThat(errors).hasSize(1)
            assertThat(errors.single()).contains("""catalog="$mount"""", """reason="unavailable"""")
            assertThat(metrics.single { it.startsWith("hoglake_mount_proxy_requests_total{") })
                .contains("""catalog="$mount"""").endsWith(" 4.0")
            assertThat(metrics.filter { it.startsWith("hoglake_mount_proxy_requests_seconds_count{") }).hasSize(1)
            assertThat(metrics.filter { it.startsWith("hoglake_mount_proxy") }.joinToString("\n"))
                .doesNotContain("team", "\"7\"", "\"42\"")
        }
    }

    @Test
    fun `a client with no postgres serves the mount to a token that carries no routing`() {
        host().use { host ->
            postgresFreeClient(host.port).use { client ->
                val token = reader()
                for (path in listOf(
                    "/v1/catalogs/$mount",
                    "/v1/catalogs/$mount/namespaces",
                    "/v1/catalogs/$mount/namespaces/ingest",
                    "/v1/catalogs/$mount/namespaces/ingest/tables",
                    "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2",
                    scan,
                    "/v1/catalogs",
                )) {
                    val direct = get(host.port, path, token)
                    val proxied = get(client.port, path, token)
                    assertThat(proxied.statusCode()).describedAs(path).isEqualTo(200)
                    assertThat(proxied.body()).describedAs(path).isEqualTo(direct.body())
                }
                val files = mapper.readTree(get(client.port, scan, token).body())
                assertThat(files.map { it["team_id"].asLong() }.toSet()).containsExactlyInAnyOrder(7L, 42L)
                // Everything else on this server is as before: nothing holds another name.
                assertThat(get(client.port, "/v1/catalogs/lake", operator()).statusCode()).isEqualTo(404)
                val before = host.received.get()
                assertThat(get(client.port, scan, reader("""{"lake":["read"]}""")).statusCode()).isEqualTo(404)
                assertThat(
                    send(client.port, HttpMethod.Post, "/v1/catalogs/$mount/commit", token).statusCode(),
                ).isEqualTo(403)
                assertThat(host.received.get()).isEqualTo(before)
            }
        }
    }
}
