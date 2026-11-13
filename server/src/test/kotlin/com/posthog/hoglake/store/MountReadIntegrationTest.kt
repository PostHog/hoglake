package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files

/**
 * The mount (export) read surface end to end (plan row 1.3h): the five
 * scoped read routes over a team-partitioned source table, through the real
 * app and its migrated database. ReadScope is proven to scope the scan, each
 * file to carry its team id, and the budget / deadline / shared cap to
 * refuse with their typed errors and count on `hoglake_mount_refusals_total`.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MountReadIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-mount-read-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mapper = wireObjectMapper()
    private val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
    private val source = "millrace-dev"

    private val sourceTableUuid: java.util.UUID
    private val teamFieldId: Long

    /** The mount's values: teams the export admits (49, outside the token, is never served). */
    private val registry =
        MountRegistry {
            if (it == mount) MountRecord(it, revision = 1, enabled = true, values = listOf(7, 42, 49, 99)) else null
        }

    init {
        val catalogs = CatalogService(db.jdbi)
        catalogs.createCatalog(source, "s3://bucket/millrace/$source")
        catalogs.createNamespace(source, "ingest")
        catalogs.createNamespace(source, "other")
        val events = catalogs.createTable(source, "ingest", "events_v2", listOf(ColumnDef("team_id", ColType.LONG)))
        sourceTableUuid = events.tableUuid
        teamFieldId = events.columns.single { it.def.name == "team_id" }.fieldId
        // A sibling table in the source catalog the mount must NOT expose: a
        // mount reader naming it gets a 404, never its schema or files.
        catalogs.createTable(source, "ingest", "other_events", listOf(ColumnDef("secret", ColType.LONG)))
        db.jdbi.useHandle<Exception> { h ->
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
                "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) " +
                    "VALUES (?, ?, 1, 0)",
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
                state: String = "provided",
                quarantined: Boolean = false,
            ) {
                h.execute(
                    "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, " +
                        "end_snapshot, path, record_count, file_size_bytes, row_id_start, spec_id, " +
                        "stats_state, prefix_quarantined) VALUES (?, ?, ?, 1, NULL, ?, ?, ?, ?, 1, ?, ?)",
                    catId,
                    id,
                    events.tableId,
                    "s3://bucket/millrace/$source/data/ingest/events_v2/team_id=$team/f$id.parquet",
                    id * 100,
                    id * 10,
                    id,
                    state,
                    quarantined,
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
            file(5, "42", state = "pending") // out of scope: stats not proven
            file(6, "42", quarantined = true) // out of scope: footer did not prove the team
            // A deletion vector on file 1, live at head, must ride it in the scan.
            h.execute(
                "INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, " +
                    "begin_snapshot, end_snapshot, path, file_format, delete_count, file_size_bytes) " +
                    "VALUES (?, 9001, ?, 1, 1, NULL, ?, 'puffin-dv', 3, 128)",
                catId,
                events.tableId,
                "s3://bucket/millrace/$source/data/ingest/events_v2/team_id=42/dv1.puffin",
            )
            // The instance kill switch is migrated off; the export serves only while it is on.
            h.execute("UPDATE hog_mount_switch SET enabled = true")
        }
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun <T> withServer(
        fileBudget: Int = 2_000_000,
        deadlineMs: Long = 60_000,
        maxConcurrent: Int = 8,
        block: (port: Int, app: App) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                mountSourceCatalog = source,
                mountSourceTableUuid = sourceTableUuid.toString(),
                mountSourceFieldId = teamFieldId.toString(),
                mountScanFileBudget = fileBudget,
                mountScanDeadlineMs = deadlineMs,
                mountScanMaxConcurrent = maxConcurrent,
                authMode = "enforce",
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
            )
        val app = App.build(cfg, db.jdbi, mountRegistry = registry)
        val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        try {
            server.start(wait = false)
            return block(runBlocking { server.engine.resolvedConnectors().first().port }, app)
        } finally {
            server.stop(0, 1000)
            app.close()
        }
    }

    /** A scoped reader of the mount, with [prd] on team_id (default: teams 7 and 42, within the export's values). */
    private fun reader(
        prd: String = """{"team_id":{"7":"read","42":"read"}}""",
        extra: String = "",
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(
                prd = prd,
                catalogs = """{"$mount":["read"]}""",
                iat = iat,
                exp = iat + 900,
                extra = extra,
            ),
        )
    }

    private fun get(
        port: Int,
        path: String,
        token: String?,
    ): HttpResponse<String> {
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path")).GET()
        token?.let { request.header("Authorization", "Bearer $it") }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString())
    }

    @Test
    fun `namespaces and namespace expose only the pinned table's namespace`() {
        withServer { port, _ ->
            val ns = get(port, "/v1/catalogs/$mount/namespaces", reader())
            assertThat(ns.statusCode()).isEqualTo(200)
            val names = mapper.readTree(ns.body()).map { it["name"].asText() }
            assertThat(names).containsExactly("ingest")
            assertThat(get(port, "/v1/catalogs/$mount/namespaces/ingest", reader()).statusCode()).isEqualTo(200)
            // 'other' exists in the source but is not the pinned table's namespace.
            assertThat(get(port, "/v1/catalogs/$mount/namespaces/other", reader()).statusCode()).isEqualTo(404)
        }
    }

    @Test
    fun `tables lists the pinned table with scoped totals, table omits totals`() {
        withServer { port, _ ->
            val tables = mapper.readTree(get(port, "/v1/catalogs/$mount/namespaces/ingest/tables", reader()).body())
            assertThat(tables.map { it["name"].asText() }).containsExactly("events_v2")
            val summary = tables.single()
            // Scope is {7,42}: files 1,2 (team 42) and 3 (team 7) are provided; 4 (99) is out of the token,
            // 5 pending and 6 quarantined are out of scope. 3 files, record_count 100+200+300.
            assertThat(summary["file_count"].asLong()).isEqualTo(3)
            assertThat(summary["record_count"].asLong()).isEqualTo(600)
            assertThat(summary["table_uuid"].asText()).isEqualTo(sourceTableUuid.toString())

            val table =
                mapper.readTree(
                    get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2", reader()).body(),
                )
            assertThat(table["name"].asText()).isEqualTo("events_v2")
            assertThat(table["namespace"].asText()).isEqualTo("ingest")
            // Totals omitted on a read-only mount (NON_NULL drops them).
            assertThat(table.has("record_count")).isFalse()
            // A table the mount does not pin is a 404.
            assertThat(
                get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/other", reader()).statusCode(),
            ).isEqualTo(404)
        }
    }

    @Test
    fun `scan returns only the scoped teams' files, each carrying its team id`() {
        withServer { port, _ ->
            val scan =
                mapper.readTree(
                    get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan", reader()).body(),
                )
            // Scope {7,42}: files 1, 2 (42) and 3 (7). Not 4 (team 99, outside the token), nor pending/quarantined.
            val byTeam = scan.groupBy { it["team_id"].asLong() }
            assertThat(byTeam.keys).containsExactlyInAnyOrder(7L, 42L)
            assertThat(scan.count()).isEqualTo(3)
            // Every file carries the team id its path is under.
            scan.forEach { f ->
                val team = f["team_id"].asLong()
                assertThat(f["data_file"]["path"].asText()).contains("team_id=$team/")
            }
            // The deletion vector on file 1 rides it.
            val file1 = scan.single { it["data_file"]["data_file_id"].asLong() == 1L }
            assertThat(file1["delete_file"]["delete_file_id"].asLong()).isEqualTo(9001)
        }
    }

    @Test
    fun `a star grant reads the whole export, a team outside the export is never served`() {
        withServer { port, _ ->
            // `*` means the export's values {7,42,49,99}; 49 has no files, 99 has file 4.
            val scan =
                mapper.readTree(
                    get(
                        port,
                        "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan",
                        reader(prd = """{"team_id":{"*":"read"}}"""),
                    ).body(),
                )
            assertThat(scan.map { it["team_id"].asLong() }.toSet()).containsExactlyInAnyOrder(7L, 42L, 99L)
            // A token naming a team OUTSIDE the export (123) intersects to nothing it did not already grant;
            // 123 is not in the export, so it is never served even though the token names it.
            val crafted =
                mapper.readTree(
                    get(
                        port,
                        "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan",
                        reader(prd = """{"team_id":{"123":"read"}}"""),
                    ).body(),
                )
            assertThat(crafted.count()).isEqualTo(0)
        }
    }

    @Test
    fun `the file budget refuses an over-budget scan and counts it`() {
        withServer(fileBudget = 1) { port, _ ->
            val response = get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan", reader())
            assertThat(response.statusCode()).isEqualTo(422)
            assertThat(response.body()).contains("mount_scan_too_many_files")
            val reasons = refusalReasons(port)
            assertThat(reasons).containsEntry("mount_scan_too_many_files", 1)
        }
    }

    @Test
    fun `the deadline maps a statement timeout to 503 and counts it`() {
        withServer(deadlineMs = 50) { port, app ->
            // Drive the deadline deterministically: a body that sleeps past it.
            assertThatThrownBy {
                app.mountReadServiceForTest.inScanTransaction(MountRecord(mount, 1, true, listOf(42L))) { h ->
                    h.execute("SELECT pg_sleep(0.3)")
                }
            }.isInstanceOf(HoglakeException.MountScanTimeout::class.java)
            assertThat(refusalReasons(port)).containsEntry("mount_scan_timeout", 1)
        }
    }

    @Test
    fun `the shared cap sheds a scan with a typed 503 and counts it`() {
        withServer(maxConcurrent = 1) { port, app ->
            // Hold the one permit so the next scan finds the cap full.
            app.mountReadServiceForTest.scanCap!!.acquire()
            try {
                val response = get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan", reader())
                assertThat(response.statusCode()).isEqualTo(503)
                assertThat(response.body()).contains("mount_scan_busy")
                assertThat(response.headers().firstValue("Retry-After")).isPresent()
                assertThat(refusalReasons(port)).containsEntry("mount_scan_busy", 1)
            } finally {
                app.mountReadServiceForTest.scanCap!!.release()
            }
        }
    }

    @Test
    fun `a served scan is counted on the per-mount served metric`() {
        withServer { port, _ ->
            get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/events_v2/scan", reader())
            val served =
                get(port, "/metrics", operator()).body().lines()
                    .single { it.startsWith("hoglake_mount_scans_total{") }
            assertThat(served).contains("""catalog="$mount"""")
            assertThat(served.substringAfterLast(' ').toDouble()).isGreaterThanOrEqualTo(1.0)
        }
    }

    @Test
    fun `the mount exposes only the pinned table, never a sibling in the source catalog`() {
        withServer { port, _ ->
            // other_events is a real table of the source catalog, not the pin.
            assertThat(get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/other_events", reader()).statusCode())
                .isEqualTo(404)
            assertThat(
                get(port, "/v1/catalogs/$mount/namespaces/ingest/tables/other_events/scan", reader()).statusCode(),
            ).isEqualTo(404)
            // The tables listing shows only the pinned table.
            val tables = mapper.readTree(get(port, "/v1/catalogs/$mount/namespaces/ingest/tables", reader()).body())
            assertThat(tables.map { it["name"].asText() }).containsExactly("events_v2")
        }
    }

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun refusalReasons(port: Int): Map<String, Int> =
        get(port, "/metrics", operator()).body().lines()
            .filter { it.startsWith("hoglake_mount_refusals_total{") }
            .associate { line ->
                val reason = Regex("""reason="([^"]+)"""").find(line)!!.groupValues[1]
                reason to line.substringAfterLast(' ').toDouble().toInt()
            }
}
