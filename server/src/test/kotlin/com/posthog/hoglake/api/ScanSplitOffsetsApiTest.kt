package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.persistence.getBigintListOrNull
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * `split_offsets` over the wire, both directions: a footer-shipping
 * registration's optional list (validated at commit, stored verbatim)
 * and GET .../scan's `include=split_offsets` (served verbatim, absent
 * when unknown). The hydrator's and compaction's lists are pinned where
 * those writers are tested; this class owns the commit contract and the
 * scan-plan surface, following the column_stats tests in
 * FileStatsApiTest: raw JSON, and the listing and changefeed checked for
 * the property's ABSENCE (spec: "Populated by GET .../scan ONLY").
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScanSplitOffsetsApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalog = "splitoffsets"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
        }

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    /** Create a one-column table; returns the column's field id. */
    private suspend fun createTable(
        client: HttpClient,
        name: String,
    ): Long {
        val response = client.postJson(tablesUrl, """{"name": "$name", "columns": [{"name": "a", "type": "long"}]}""")
        assertThat(response.status).describedAs(response.bodyAsText()).isEqualTo(HttpStatusCode.Created)
        return body(response)["columns"][0]["field_id"].asLong()
    }

    private suspend fun commit(
        client: HttpClient,
        table: String,
        filesJson: String,
    ): HttpResponse =
        client.postJson(
            "/v1/catalogs/$catalog/commit",
            """{"appends": [{"namespace": "ns", "table": "$table", "files": [$filesJson]}]}""",
        )

    private fun JsonNode.scanFile(path: String): JsonNode =
        first { it["data_file"]["path"].asText() == path }["data_file"]

    private fun JsonNode.longs(): List<Long> = map { it.asLong() }

    /** The stored column, read past every API surface. */
    private fun storedOffsets(path: String): List<Long>? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT split_offsets FROM hog_data_file WHERE path = :path")
                .bind("path", path)
                .map { rs, _ -> rs.getBigintListOrNull("split_offsets") }
                .one()
        }

    @Test
    fun `include=split_offsets serves a registration's list verbatim, and nothing else carries it`() =
        api { client ->
            val fieldId = createTable(client, "served")
            val withOffsets = "s3://b/$catalog/served/with.parquet"
            val without = "s3://b/$catalog/served/without.parquet"
            val pendingWith = "s3://b/$catalog/served/pending.parquet"
            val stats = """"column_stats": [{"field_id": $fieldId, "value_count": 3, "null_count": 0}]"""
            val response =
                commit(
                    client,
                    "served",
                    """{"path": "$withOffsets", "record_count": 3, "file_size_bytes": 10000,
                        "split_offsets": [4, 1000, 5000], $stats},
                       {"path": "$without", "record_count": 3, "file_size_bytes": 10000, $stats},
                       {"path": "$pendingWith", "record_count": 3, "file_size_bytes": 1024,
                        "split_offsets": [4]}""",
                )
            assertThat(response.status).describedAs(response.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            assertThat(storedOffsets(withOffsets)).containsExactly(4L, 1000L, 5000L)
            // Absent at registration is NULL, not an empty array.
            assertThat(storedOffsets(without)).isNull()

            // Opt-in: a plain scan carries it for no file.
            val plain = body(client.get("$tablesUrl/served/scan"))
            assertThat(plain).hasSize(3)
            assertThat(plain.map { it["data_file"].has("split_offsets") }).containsOnly(false)

            val scan = body(client.get("$tablesUrl/served/scan?include=split_offsets"))
            assertThat(scan.scanFile(withOffsets)["split_offsets"].longs()).containsExactly(4L, 1000L, 5000L)
            // No stored list: the property is ABSENT, never [] or null.
            assertThat(scan.scanFile(without).has("split_offsets")).isFalse()
            // Independent of stats_state: a pending file's shipped list is served.
            assertThat(scan.scanFile(pendingWith)["stats_state"].asText()).isEqualTo("pending")
            assertThat(scan.scanFile(pendingWith)["split_offsets"].longs()).containsExactly(4L)
            // One part asked for, one part served.
            assertThat(scan.map { it["data_file"].has("column_stats") }).containsOnly(false)

            // Combined, in one occurrence or two: both parts.
            for (query in listOf("include=column_stats,split_offsets", "include=column_stats&include=split_offsets")) {
                val both = body(client.get("$tablesUrl/served/scan?$query"))
                val file = both.scanFile(withOffsets)
                assertThat(file["split_offsets"].longs()).describedAs(query).containsExactly(4L, 1000L, 5000L)
                assertThat(
                    file["column_stats"].map { it["field_id"].asLong() },
                ).describedAs(query).containsExactly(fieldId)
                assertThat(both.scanFile(without).has("split_offsets")).isFalse()
            }
            // column_stats alone does not bring the offsets along.
            val statsOnly = body(client.get("$tablesUrl/served/scan?include=column_stats"))
            assertThat(statsOnly.map { it["data_file"].has("split_offsets") }).containsOnly(false)

            // The listing and the changefeed embed the same DataFile
            // schema and never carry it.
            val listed = body(client.get("$tablesUrl/served/files"))
            assertThat(listed).hasSize(3)
            assertThat(listed.map { it.has("split_offsets") }).containsOnly(false)
            val changes = body(client.get("$tablesUrl/served/changes?from_snapshot=0"))
            assertThat(changes["files"]).hasSize(3)
            assertThat(changes["files"].map { it.has("split_offsets") }).containsOnly(false)
        }

    @Test
    fun `an invalid list is a 422 and the commit stores nothing`() =
        api { client ->
            createTable(client, "refused")
            val good =
                """{"path": "s3://b/$catalog/refused/good.parquet", "record_count": 1, "file_size_bytes": 1024}"""
            val bad =
                mapOf(
                    "unsorted" to "[4, 500, 100]",
                    "duplicate entry" to "[4, 4]",
                    "negative" to "[-1, 4]",
                    "at file_size_bytes" to "[4, 1024]",
                    "past file_size_bytes" to "[4, 5000]",
                    "empty" to "[]",
                )
            for ((label, list) in bad) {
                val file =
                    """{"path": "s3://b/$catalog/refused/bad.parquet", "record_count": 1,
                        "file_size_bytes": 1024, "split_offsets": $list}"""
                // Alongside a valid file: the refusal is the whole commit.
                val response = commit(client, "refused", "$good, $file")
                assertThat(response.status).describedAs("$label: ${response.bodyAsText()}")
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
                val error = body(response)
                assertThat(error["error"].asText()).isEqualTo("validation")
                assertThat(error["detail"].asText()).describedAs(label).contains("split_offsets")
            }
            assertThat(body(client.get("$tablesUrl/refused/files"))).isEmpty()
            assertThat(
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(*) FROM hog_data_file WHERE path LIKE '%/refused/%'")
                        .mapTo(Long::class.java).one()
                },
            ).isZero()
            // The boundary on the accepted side: last entry one byte below the size.
            val edge =
                commit(
                    client,
                    "refused",
                    """{"path": "s3://b/$catalog/refused/edge.parquet", "record_count": 1,
                        "file_size_bytes": 1024, "split_offsets": [0, 1023]}""",
                )
            assertThat(edge.status).describedAs(edge.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            assertThat(storedOffsets("s3://b/$catalog/refused/edge.parquet")).containsExactly(0L, 1023L)
        }

    @Test
    fun `include stays strict - unknown values and an orphan stats_fields are still 422`() =
        api { client ->
            createTable(client, "strict")
            val base = "$tablesUrl/strict/scan"
            val unknown = client.get("$base?include=split_offset")
            assertThat(unknown.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(unknown.bodyAsText()).contains("split_offset").contains("column_stats, split_offsets")
            assertThat(client.get("$base?include=split_offsets,bogus").status)
                .isEqualTo(HttpStatusCode.UnprocessableEntity)
            // stats_fields narrows STATISTICS; split_offsets is not a
            // statistics request, so it does not license stats_fields.
            val orphan = client.get("$base?include=split_offsets&stats_fields=1")
            assertThat(orphan.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(orphan.bodyAsText()).contains("include=column_stats")
            assertThat(client.get("$base?include=split_offsets").status).isEqualTo(HttpStatusCode.OK)
            assertThat(client.get("$base?include=split_offsets,split_offsets").status).isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `atomic table creation validates and stores the list like a commit`() =
        api { client ->
            suspend fun prepare(name: String): Pair<String, String> {
                val path = "/v1/catalogs/$catalog/table-creations/${UUID.randomUUID()}"
                val response =
                    client.put(path) {
                        contentType(ContentType.Application.Json)
                        setBody("""{"namespace": "ns", "name": "$name", "columns": [{"name": "a", "type": "long"}]}""")
                    }
                assertThat(response.status).describedAs(response.bodyAsText()).isEqualTo(HttpStatusCode.OK)
                return path to body(response)["write_path"].asText().trimEnd('/')
            }

            suspend fun publish(
                path: String,
                file: String,
                offsets: String,
            ): HttpResponse =
                client.postJson(
                    "$path/commit",
                    """{"files": [{"path": "$file", "record_count": 1, "file_size_bytes": 1024,
                        "footer_size": 100, "split_offsets": $offsets}]}""",
                )

            val (refusedPath, refusedWrite) = prepare("created_refused")
            val refused = publish(refusedPath, "$refusedWrite/f.parquet", "[500, 4]")
            assertThat(refused.status).describedAs(refused.bodyAsText()).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(refused.bodyAsText()).contains("split_offsets")

            val (okPath, okWrite) = prepare("created_ok")
            val ok = publish(okPath, "$okWrite/f.parquet", "[4, 500]")
            assertThat(ok.status).describedAs(ok.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            assertThat(body(ok)["state"].asText()).isEqualTo("committed")
            val scan = body(client.get("$tablesUrl/created_ok/scan?include=split_offsets"))
            assertThat(scan.single()["data_file"]["split_offsets"].longs()).containsExactly(4L, 500L)
        }
}
