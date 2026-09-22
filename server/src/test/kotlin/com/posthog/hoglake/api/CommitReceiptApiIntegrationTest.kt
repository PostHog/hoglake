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
    fun `prepared delete requires guards and replays a lost response`() {
        PgTestSupport.freshDatabase().use { db ->
            testApplication {
                application { App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi).module(this) }

                suspend fun post(
                    path: String,
                    body: String,
                ) = client.post(path) {
                    contentType(ContentType.Application.Json)
                    setBody(body)
                }
                val base = "/v1/catalogs/delete-test"
                val json = ObjectMapper()
                post("/v1/catalogs", """{"name":"delete-test","data_path":"s3://synthetic/"}""")
                post("$base/namespaces", """{"name":"ns"}""")
                val table =
                    json.readTree(
                        post(
                            "$base/namespaces/ns/tables",
                            """{"name":"target","columns":[{"name":"id","type":"long"}]}""",
                        ).bodyAsText(),
                    )
                val uuid = table["table_uuid"].asText()
                val appended =
                    json.readTree(
                        post(
                            "$base/commit",
                            """{"appends":[{"namespace":"ns","table":"target",
                    "files":[{"path":"s3://synthetic/data","record_count":7,"file_size_bytes":700}]}]}""",
                        ).bodyAsText(),
                    )
                val snapshot = appended["snapshot_id"].asLong()
                val key = java.util.UUID.randomUUID()
                val request = """{"idempotency_key":"$key","read_snapshot":$snapshot,"deletes":[{
                    "namespace":"ns","table":"target","expected_table_uuid":"$uuid",
                    "files":[{"data_file_id":1,"path":"s3://synthetic/dv","delete_count":2,"file_size_bytes":100}]}]}"""
                assertThat(json.readTree(client.get(base).bodyAsText())["capabilities"].map { it.asText() })
                    .contains("idempotent-delete-v1")
                assertThat(
                    post("$base/commit/deletes/prepared", "{}").status,
                ).isEqualTo(HttpStatusCode.UnprocessableEntity)
                val empty = """{"idempotency_key":"${java.util.UUID.randomUUID()}","read_snapshot":$snapshot,
                    "deletes":[{"namespace":"ns","table":"target","expected_table_uuid":"$uuid","files":[]}]}"""
                assertThat(post("$base/commit/deletes/prepared", empty).status).isEqualTo(HttpStatusCode.OK)
                assertThat(post("$base/commit", empty).status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                val result = post("$base/commit/deletes/prepared", request)
                assertThat(result.status).isEqualTo(HttpStatusCode.OK)
                val committed = json.readTree(result.bodyAsText())["snapshot_id"].asLong()
                assertThat(
                    json.readTree(post("$base/commit/deletes/prepared", request).bodyAsText())["snapshot_id"].asLong(),
                )
                    .isEqualTo(committed)
                assertThat(json.readTree(client.get("$base/commit/receipts/$key").bodyAsText())["snapshot_id"].asLong())
                    .isEqualTo(committed)
                assertThat(
                    post(
                        "$base/commit/deletes/prepared",
                        request.replace("\"delete_count\":2", "\"delete_count\":3"),
                    ).status,
                )
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
            }
        }
    }

    @Test
    fun `prepared mutation atomically publishes mixed files and fences stale target reads`() {
        PgTestSupport.freshDatabase().use { db ->
            testApplication {
                application { App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi).module(this) }

                suspend fun post(
                    path: String,
                    body: String,
                ) = client.post(path) {
                    contentType(ContentType.Application.Json)
                    setBody(body)
                }
                val base = "/v1/catalogs/mutation-test"
                val json = ObjectMapper()
                post("/v1/catalogs", """{"name":"mutation-test","data_path":"s3://synthetic/"}""")
                post("$base/namespaces", """{"name":"ns"}""")
                val table =
                    json.readTree(
                        post(
                            "$base/namespaces/ns/tables",
                            """{"name":"target","columns":[{"name":"id","type":"long"}]}""",
                        ).bodyAsText(),
                    )
                val uuid = table["table_uuid"].asText()
                val initial =
                    json.readTree(
                        post(
                            "$base/commit",
                            """{"appends":[{"namespace":"ns","table":"target",
                    "files":[{"path":"s3://synthetic/old","record_count":7,"file_size_bytes":700}]}]}""",
                        ).bodyAsText(),
                    )
                val snapshot = initial["snapshot_id"].asLong()
                val key = java.util.UUID.randomUUID()
                val request = """{"idempotency_key":"$key","read_snapshot":$snapshot,
                    "appends":[{"namespace":"ns","table":"target","expected_table_uuid":"$uuid","files":[
                    {"path":"s3://synthetic/new1","record_count":2,"file_size_bytes":200},
                    {"path":"s3://synthetic/new2","record_count":1,"file_size_bytes":100}]}],
                    "deletes":[{"namespace":"ns","table":"target","expected_table_uuid":"$uuid",
                    "files":[{"data_file_id":1,"path":"s3://synthetic/dv","delete_count":2,"file_size_bytes":100}]}]}"""
                assertThat(json.readTree(client.get(base).bodyAsText())["capabilities"].map { it.asText() })
                    .contains("idempotent-mutation-v1")
                assertThat(
                    post("$base/commit/mutations/prepared", "{}").status,
                ).isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(
                    post("$base/commit/deletes/prepared", request).status,
                ).isEqualTo(HttpStatusCode.UnprocessableEntity)
                // A late validation failure rolls back the appends too.
                assertThat(
                    post(
                        "$base/commit/mutations/prepared",
                        request.replace("\"data_file_id\":1", "\"data_file_id\":99"),
                    ).status,
                )
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(
                    json.readTree(client.get(base).bodyAsText())["head_snapshot_id"].asLong(),
                ).isEqualTo(snapshot)
                val result = post("$base/commit/mutations/prepared", request)
                assertThat(result.status).isEqualTo(HttpStatusCode.OK)
                val committed = json.readTree(result.bodyAsText())["snapshot_id"].asLong()
                assertThat(committed).isEqualTo(snapshot + 1)
                assertThat(
                    json.readTree(
                        post("$base/commit/mutations/prepared", request).bodyAsText(),
                    )["snapshot_id"].asLong(),
                )
                    .isEqualTo(committed)
                assertThat(post("$base/commit/mutations/prepared", request.replace("new2", "changed")).status)
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
                val target = "$base/namespaces/ns/tables/target"
                assertThat(json.readTree(client.get(target).bodyAsText())["record_count"].asLong()).isEqualTo(10)
                assertThat(
                    json.readTree(client.get("$target?snapshot=$snapshot").bodyAsText())["record_count"].asLong(),
                ).isEqualTo(7)
                val changes =
                    json.readTree(
                        client.get("$target/changes?from_snapshot=$snapshot&to_snapshot=$committed").bodyAsText(),
                    )
                assertThat(changes["files"].size()).isEqualTo(2)
                assertThat(changes["delete_files"].size()).isEqualTo(1)
                assertThat(changes["files"].sumOf { it["record_count"].asLong() }).isEqualTo(3)
                assertThat(changes["delete_files"][0]["delete_count"].asLong()).isEqualTo(2)
                // Empty delete groups anchor insert-only and no-op statements to the read snapshot.
                val empty = """{"idempotency_key":"${java.util.UUID.randomUUID()}","read_snapshot":$snapshot,
                    "deletes":[{"namespace":"ns","table":"target","expected_table_uuid":"$uuid","files":[]}]}"""
                val stale = post("$base/commit/mutations/prepared", empty)
                assertThat(stale.status).isEqualTo(HttpStatusCode.Conflict)
                assertThat(
                    post(
                        "$base/commit/mutations/prepared",
                        empty.replace("\"read_snapshot\":$snapshot", "\"read_snapshot\":$committed"),
                    ).status,
                )
                    .isEqualTo(HttpStatusCode.OK)
                db.jdbi.useHandle<Exception> { h ->
                    assertThat(
                        h.createQuery("SELECT COUNT(*) FROM hog_data_file").mapTo(Long::class.java).one(),
                    ).isEqualTo(3)
                    assertThat(
                        h.createQuery(
                            "SELECT COUNT(DISTINCT begin_snapshot) FROM hog_data_file WHERE data_file_id > 1",
                        ).mapTo(Long::class.java).one(),
                    )
                        .isEqualTo(1)
                    assertThat(
                        h.createQuery("SELECT COUNT(*) FROM hog_snapshot_change WHERE snapshot_id = :snapshot")
                            .bind("snapshot", committed).mapTo(Long::class.java).one(),
                    ).isEqualTo(2)
                }
            }
        }
    }

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
