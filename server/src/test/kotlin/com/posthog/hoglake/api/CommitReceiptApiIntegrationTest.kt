package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class CommitReceiptApiIntegrationTest {
    @Test
    fun `receipt lookup survives application restart and is catalog scoped`() {
        PgTestSupport.freshDatabase().use { db ->
            val key = "12345678-1234-5678-90ab-1234567890ab"
            val base = "/v1/catalogs/receipt-test"
            val json = ObjectMapper()
            var expectedSnapshot = 0L
            testApplication {
                application { App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi).module(this) }

                suspend fun post(
                    path: String,
                    body: String,
                ) = client.post(path) {
                    contentType(ContentType.Application.Json)
                    setBody(body)
                }
                assertThat(post("/v1/catalogs", """{"name":"receipt-test","data_path":"s3://synthetic/"}""").status)
                    .isEqualTo(HttpStatusCode.Created)
                post("$base/namespaces", """{"name":"ns"}""")
                post("$base/namespaces/ns/tables", """{"name":"target","columns":[{"name":"id","type":"long"}]}""")
                val capabilities = json.readTree(client.get(base).bodyAsText())["capabilities"].map { it.asText() }
                assertThat(capabilities).contains("idempotent-append-v1")
                assertThat(client.get("$base/commit/receipts/$key").status).isEqualTo(HttpStatusCode.NotFound)
                val request = """{"idempotency_key":"$key","appends":[{"namespace":"ns","table":"target","files":[
                    {"path":"s3://synthetic/data.parquet","record_count":7,"file_size_bytes":700}]}]}"""
                val committed = post("$base/commit", request)
                assertThat(committed.status).isEqualTo(HttpStatusCode.OK)
                expectedSnapshot = json.readTree(committed.bodyAsText())["snapshot_id"].asLong()
                assertThat(json.readTree(post("$base/commit", request).bodyAsText())["snapshot_id"].asLong())
                    .isEqualTo(expectedSnapshot)
                assertThat(client.get("$base/commit/receipts/not-a-uuid").status).isEqualTo(HttpStatusCode.BadRequest)
            }
            // A new application has no knowledge of the previous HTTP requests.
            testApplication {
                application { App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi).module(this) }
                val response = client.get("$base/commit/receipts/$key")
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                val receipt = json.readTree(response.bodyAsText())
                assertThat(receipt["operation_id"].asText()).isEqualTo(key)
                assertThat(receipt["snapshot_id"].asLong()).isEqualTo(expectedSnapshot)
                assertThat(receipt.has("request")).isFalse()
                assertThat(
                    client.get("/v1/catalogs/another/commit/receipts/$key").status,
                ).isEqualTo(HttpStatusCode.NotFound)
            }
            db.jdbi.useHandle<Exception> { h ->
                assertThat(
                    h.createQuery("SELECT COUNT(*) FROM hog_data_file").mapTo(Long::class.java).one(),
                ).isEqualTo(1)
                assertThat(h.createQuery("SELECT SUM(record_count) FROM hog_data_file").mapTo(Long::class.java).one())
                    .isEqualTo(7)
            }
        }
    }
}
