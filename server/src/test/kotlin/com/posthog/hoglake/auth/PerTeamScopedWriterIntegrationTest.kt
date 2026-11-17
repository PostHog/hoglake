package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.UUID

/**
 * The Gigahog imports writer: one per-team scoped token (`ten: scoped`, the
 * org, `cat` write on the imports catalog, `prd` write on one team, no
 * `rtg` or `dlg`) against an org-marked `team_<id>` namespace an operator
 * made. It never creates a namespace. Over HTTP it reads the namespace,
 * creates a table, claims and uploads parquet under the org prefix, reads
 * the table back, and replaces its files in one prepared commit whose
 * stats are hydrated from the footers.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PerTeamScopedWriterIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-team-writer-", ".json").also {
            Files.writeString(it, TestTokens.jwks(pair))
        }
    private val json = wireObjectMapper()
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val cat = "imports-dev"
    private val dataPath = "s3://$BUCKET/$cat"
    private val org = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val otherOrg = "02b2f6ef-2bd6-0000-f64f-e3a0b88efbf5"
    private val team = 5L
    private val rows = 20

    private companion object {
        const val BUCKET = "hoglake-test"

        val minio by lazy { TestImages.minio().also { it.start() } }

        val store: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }
    }

    init {
        catalogs.createCatalog(cat, dataPath)
        // team_5 is the writer's; team_6 is another team of the same org; team_77's mark is another org's.
        for ((name, value, markOrg) in listOf(Triple("team_5", 5L, org), Triple("team_6", 6L, org))) {
            catalogs.createNamespace(cat, name)
            catalogs.setNamespaceContainment(cat, name, NamespaceContainment("team_id", value, markOrg))
        }
        catalogs.createNamespace(cat, "team_8")
        catalogs.setNamespaceContainment(cat, "team_8", NamespaceContainment("team_id", 8, org))
        catalogs.createNamespace(cat, "team_77")
        catalogs.setNamespaceContainment(cat, "team_77", NamespaceContainment("team_id", 77, otherOrg))
        for (ns in listOf("team_6", "team_8", "team_77")) {
            catalogs.createTable(cat, ns, "events", listOf(ColumnDef("id", ColType.LONG)))
        }
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    /** The writer's token: no `rtg`, no `dlg`, and `prd` write on [teams] only. */
    private fun writer(
        teams: List<Long> = listOf(team),
        tokenOrg: String = org,
    ): String {
        val prd = teams.joinToString(",", prefix = "{\"team_id\":{", postfix = "}}") { "\"$it\":\"write\"" }
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(
            pair,
            TestTokens.scopedClaims(prd = prd, catalogs = """{"$cat":["write"]}""", iat = iat, org = tokenOrg),
        )
    }

    private fun <T> withServer(block: (port: Int) -> T): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = AuthMode.ENFORCE.wire,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
            )
        val app = App.build(cfg, db.jdbi)
        val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        try {
            server.start(wait = false)
            return block(runBlocking { server.engine.resolvedConnectors().first().port })
        } finally {
            server.stop(0, 1000)
            app.close()
        }
    }

    private fun call(
        port: Int,
        token: String,
        method: String,
        path: String,
        body: String? = null,
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/catalogs/$cat$path"))
                .header("Authorization", "Bearer $token")
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun tree(
        response: HttpResponse<String>,
        expected: Int = 200,
    ): JsonNode {
        assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(expected)
        return json.readTree(response.body())
    }

    private fun assertMissing(
        response: HttpResponse<String>,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(404)
        assertThat(response.body()).describedAs(what).doesNotContain("team_id").doesNotContain("scoped credential")
    }

    private fun parquet(
        fieldId: Long,
        base: Long,
    ): ByteArray {
        val schema =
            MessageType(
                "events",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(fieldId.toInt()).named("id"),
            )
        val tmp = Files.createTempFile("hoglake-team-writer", ".parquet")
        try {
            Files.deleteIfExists(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { writer -> for (i in 0 until rows) writer.write(factory.newGroup().append("id", base + i)) }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private fun footerSize(bytes: ByteArray): Long =
        ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()

    private fun registration(
        path: String,
        bytes: ByteArray,
        recordCount: Int = rows,
    ) = """{"path":"$path","record_count":$recordCount,"file_size_bytes":${bytes.size},""" +
        """"footer_size":${footerSize(bytes)}}"""

    /** A replacement that ends every live file of [ns].events and appends [files]. */
    private fun replacement(
        ns: String,
        tableUuid: String,
        readSnapshot: Long,
        files: List<String>,
        key: UUID = UUID.randomUUID(),
    ) = """{"idempotency_key":"$key","read_snapshot":$readSnapshot,"message":"full replacement",""" +
        """"appends":[{"namespace":"$ns","table":"events","expected_table_uuid":"$tableUuid",""" +
        """"files":[${files.joinToString(",")}]}],""" +
        """"file_ends":[{"namespace":"$ns","table":"events","expected_table_uuid":"$tableUuid","all_files":true}]}"""

    private fun statsStates(): Map<String, String> =
        db.jdbi.withHandle<Map<String, String>, Exception> { h ->
            h.createQuery(
                """
                SELECT f.path, f.stats_state FROM hog_data_file f
                JOIN hog_catalog c USING (catalog_id) WHERE c.name = ? AND f.end_snapshot IS NULL
                """,
            ).bind(0, cat)
                .map { rs, _ -> rs.getString(1) to rs.getString(2) }
                .list().toMap()
        }

    private fun statsRowCount(path: String): Long =
        db.jdbi.withHandle<Long, Exception> { h ->
            h.createQuery(
                """
                SELECT count(*) FROM hog_file_column_stats s
                JOIN hog_data_file f USING (catalog_id, data_file_id) WHERE f.path = ?
                """,
            ).bind(0, path).mapTo(Long::class.java).one()
        }

    @Test
    fun `a per-team scoped token writes its own org-marked namespace end to end`() {
        withServer { port ->
            val token = writer()
            val prefix = "$dataPath/org_id=$org/team_id=$team"

            // 1. The namespace is the operator's, and the writer finds it.
            val namespace = tree(call(port, token, "GET", "/namespaces/team_5"))
            assertThat(namespace.path("containment").path("org").asText()).isEqualTo(org)

            // 2. Create the table: prepare, then publish with no files.
            val operation = "/table-creations/${UUID.randomUUID()}"
            val prepared =
                tree(
                    call(
                        port,
                        token,
                        "PUT",
                        operation,
                        """{"namespace":"team_5","name":"events","columns":[{"name":"id","type":"long"}]}""",
                    ),
                )
            assertThat(prepared.path("write_path").asText()).startsWith("$prefix/")
            val fieldId = prepared.path("columns").single().path("field_id").asLong()
            tree(call(port, token, "POST", "$operation/commit", """{"files":[]}"""))

            // 3. Claim an upload under the org prefix and write the parquet where the claim says.
            // A claim is owned by the idempotency key of the commit that registers it.
            fun upload(
                base: Long,
                owner: UUID,
            ): Pair<String, ByteArray> {
                val claim =
                    tree(
                        call(
                            port,
                            token,
                            "PUT",
                            "/uploads/${UUID.randomUUID()}",
                            """{"owner":"$owner","prefix":"$prefix/events","file_kind":"data"}""",
                        ),
                    )
                val path = claim.path("path").asText()
                assertThat(path).startsWith("$prefix/events/")
                val bytes = parquet(fieldId, base)
                store.put(path, bytes)
                return path to bytes
            }

            // 4. The table reads back, empty, and names what the commit guards on.
            fun table() = tree(call(port, token, "GET", "/namespaces/team_5/tables/events"))

            fun files() =
                tree(call(port, token, "GET", "/namespaces/team_5/tables/events/files")).let {
                    if (it.isArray) it.toList() else it.path("files").toList()
                }
            val first = table()
            val tableUuid = first.path("table_uuid").asText()
            assertThat(files()).isEmpty()

            // 5. One-commit full replacement, deferred stats, no author (the token names the actor).
            val aKey = UUID.randomUUID()
            val (a, aBytes) = upload(100, aKey)
            val loaded =
                tree(
                    call(
                        port,
                        token,
                        "POST",
                        "/commit/replacements/prepared",
                        replacement(
                            "team_5",
                            tableUuid,
                            first.path("read_snapshot_id").asLong(),
                            listOf(registration(a, aBytes)),
                            aKey,
                        ),
                    ),
                )
            assertThat(loaded.path("ended_files").asLong()).isZero()
            assertThat(statsStates()).containsEntry(a, "pending")
            assertThat(Hydrator(db.jdbi, store).runOnce()).isEqualTo(1)
            assertThat(statsStates()).containsEntry(a, "provided")
            assertThat(statsRowCount(a)).isPositive()

            // Replacing again ends the first load's file and leaves only the second.
            val bKey = UUID.randomUUID()
            val (b, bBytes) = upload(500, bKey)
            val second = table()
            assertThat(files().map { it.path("path").asText() }).containsExactly(a)
            val replaced =
                tree(
                    call(
                        port,
                        token,
                        "POST",
                        "/commit/replacements/prepared",
                        replacement(
                            "team_5",
                            tableUuid,
                            second.path("read_snapshot_id").asLong(),
                            listOf(registration(b, bBytes)),
                            bKey,
                        ),
                    ),
                )
            assertThat(replaced.path("ended_files").asLong()).isEqualTo(1)
            assertThat(Hydrator(db.jdbi, store).runOnce()).isEqualTo(1)
            assertThat(statsStates().keys).containsExactly(b)
            val after = table()
            assertThat(files().map { it.path("path").asText() }).containsExactly(b)
            assertThat(files().single().path("record_count").asLong()).isEqualTo(rows.toLong())

            // A registered record_count the footer does not bear out is not trusted: the file fails hydration.
            val cKey = UUID.randomUUID()
            val (c, cBytes) = upload(900, cKey)
            tree(
                call(
                    port,
                    token,
                    "POST",
                    "/commit/replacements/prepared",
                    replacement(
                        "team_5",
                        tableUuid,
                        after.path("read_snapshot_id").asLong(),
                        listOf(registration(c, cBytes, recordCount = rows + 1)),
                        cKey,
                    ),
                ),
            )
            Hydrator(db.jdbi, store).runOnce()
            assertThat(statsStates()).containsEntry(c, "failed")
            assertThat(statsRowCount(c)).isZero()
        }
    }

    @Test
    fun `the same token is refused where the namespace is not its own`() {
        withServer { port ->
            val token = writer()
            val columns = """[{"name":"id","type":"long"}]"""
            val bytes = parquet(1, 0)

            // team_6: another team of the same org. team_77: its mark is another org's, and
            // the token holds that team under this one.
            val cases =
                listOf(
                    Foreign("team_6", org, 6L, token),
                    Foreign("team_77", otherOrg, 77L, writer(listOf(77L))),
                )
            for (case in cases) {
                val create = """{"namespace":"${case.ns}","name":"made","columns":$columns}"""
                assertMissing(
                    call(port, case.token, "PUT", "/table-creations/${UUID.randomUUID()}", create),
                    "create in ${case.ns}",
                )
                val uuid = catalogs.getTable(cat, case.ns, "events").tableUuid.toString()
                val snapshot = catalogs.getCatalog(cat).headSnapshotId
                val path = "$dataPath/org_id=${case.markOrg}/team_id=${case.value}/events/x.parquet"
                assertMissing(
                    call(
                        port,
                        case.token,
                        "POST",
                        "/commit/replacements/prepared",
                        replacement(case.ns, uuid, snapshot, listOf(registration(path, bytes))),
                    ),
                    "commit to ${case.ns}",
                )
            }
        }
    }

    private data class Foreign(val ns: String, val markOrg: String, val value: Long, val token: String)

    @Test
    fun `a file outside the team's own prefix is refused`() {
        withServer { port ->
            val token = writer(listOf(8L))
            val table = tree(call(port, token, "GET", "/namespaces/team_8/tables/events"))
            val bytes = parquet(table.path("columns").single().path("field_id").asLong(), 0)
            val outside =
                listOf(
                    "$dataPath/data/team_8/events/x.parquet",
                    "$dataPath/org_id=$org/team_id=6/events/x.parquet",
                    "$dataPath/org_id=$otherOrg/team_id=8/events/x.parquet",
                    "$dataPath/org_id=$org/team_id=8evil/events/x.parquet",
                )
            for (path in outside) {
                val body =
                    replacement(
                        "team_8",
                        table.path("table_uuid").asText(),
                        table.path("read_snapshot_id").asLong(),
                        listOf(registration(path, bytes)),
                    )
                val refused = call(port, token, "POST", "/commit/replacements/prepared", body)
                assertThat(refused.statusCode()).describedAs("%s: %s", path, refused.body()).isEqualTo(422)
                assertThat(json.readTree(refused.body()).path("error").asText()).isEqualTo("containment_violation")
            }
        }
    }
}
