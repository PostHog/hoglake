package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.UUID

/**
 * Deferred stats on the PREPARED commit (`POST /commit/prepared`, what
 * pyhoglake's `prepare_append_*(deferred_stats=True)` +
 * `Catalog.commit_prepared` send). A registration without
 * `column_stats` lands `stats_state='pending'` while one with them lands
 * `'provided'` in the same commit, and one hydrator sweep fills the
 * pending file's stats from its footer — outside the commit lock, which
 * is the point of deferring them.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PreparedCommitDeferredStatsHydrationIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val hydrator by lazy { Hydrator(db.jdbi, store) }
    private val json = ObjectMapper()

    private val catalog = "prepdeferred"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    private companion object {
        const val BUCKET = "hoglake-test"
        const val ROWS = 20

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

    /** `id` = [base] until [base] + ROWS - 1, field id [idFieldId]. */
    private fun write(
        idFieldId: Int,
        base: Long,
    ): ByteArray {
        val schema =
            MessageType(
                "prep",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(idFieldId).named("id"),
            )
        val tmp = Files.createTempFile("hoglake-prepdeferred", ".parquet")
        try {
            Files.deleteIfExists(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { writer ->
                    for (i in 0 until ROWS) writer.write(factory.newGroup().append("id", base + i))
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private fun footerSizeOf(bytes: ByteArray): Long =
        ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()

    private fun statsStates(): Map<String, String> =
        db.jdbi.withHandle<Map<String, String>, Exception> { h ->
            h.createQuery("SELECT path, stats_state FROM hog_data_file")
                .map { rs, _ -> rs.getString(1) to rs.getString(2) }
                .list().toMap()
        }

    @Test
    fun `a prepared registration without column_stats lands pending and one hydrator sweep provides it`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://$BUCKET/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
            val created =
                client.postJson(tablesUrl, """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""")
            assertThat(created.status).describedAs(created.bodyAsText()).isEqualTo(HttpStatusCode.Created)
            val table = body(created)
            val tableUuid = table["table_uuid"].asText()
            val idField = table["columns"].single()["field_id"].asInt()

            val deferred = write(idField, base = 100)
            val shipped = write(idField, base = 500)
            val deferredPath = "s3://$BUCKET/$catalog/data/ns/events/deferred.parquet"
            val shippedPath = "s3://$BUCKET/$catalog/data/ns/events/shipped.parquet"
            store.put(deferredPath, deferred)
            store.put(shippedPath, shipped)
            val head = body(client.get("/v1/catalogs/$catalog"))["head_snapshot_id"].asLong()

            val commit =
                client.postJson(
                    "/v1/catalogs/$catalog/commit/prepared",
                    """{"idempotency_key": "${UUID.randomUUID()}", "read_snapshot": $head,
                        "appends": [{"namespace": "ns", "table": "events",
                                     "expected_table_uuid": "$tableUuid", "files": [
                       {"path": "$deferredPath", "record_count": $ROWS,
                        "file_size_bytes": ${deferred.size}, "footer_size": ${footerSizeOf(deferred)}},
                       {"path": "$shippedPath", "record_count": $ROWS,
                        "file_size_bytes": ${shipped.size}, "footer_size": ${footerSizeOf(shipped)},
                        "column_stats": [{"field_id": $idField, "value_count": $ROWS, "null_count": 0}]}
                    ]}]}""",
                )
            assertThat(commit.status).describedAs(commit.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            assertThat(statsStates())
                .containsExactlyInAnyOrderEntriesOf(mapOf(deferredPath to "pending", shippedPath to "provided"))

            // Exactly the pending file is the sweep's work.
            assertThat(hydrator.runOnce()).isEqualTo(1)
            assertThat(statsStates())
                .containsExactlyInAnyOrderEntriesOf(mapOf(deferredPath to "provided", shippedPath to "provided"))

            val deferredId =
                body(client.get("$tablesUrl/events/files"))
                    .single { it["path"].asText() == deferredPath }["data_file_id"].asLong()
            val row =
                db.jdbi.withHandle<Map<String, Any?>, Exception> { h ->
                    h.createQuery(
                        """
                        SELECT value_count, null_count, lower_bound, upper_bound
                        FROM hog_file_column_stats WHERE data_file_id = ? AND field_id = ?
                        """,
                    ).bind(0, deferredId).bind(1, idField.toLong()).mapToMap().one()
                }
            assertThat(row["value_count"]).isEqualTo(ROWS.toLong())
            assertThat(row["null_count"]).isEqualTo(0L)
            assertThat(row["lower_bound"] as ByteArray).isEqualTo(IcebergSingleValue.encodeLong(100))
            assertThat(row["upper_bound"] as ByteArray).isEqualTo(IcebergSingleValue.encodeLong(100L + ROWS - 1))
        }
}
