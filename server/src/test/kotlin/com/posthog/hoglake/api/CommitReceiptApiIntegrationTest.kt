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

                // A DV whose target was retired since read_snapshot — what
                // compaction does to a connector's plan — is 409 at the
                // wire, not the 422 that told it to give up. A target that
                // was ALREADY dead at read_snapshot stays 422.
                val second =
                    json.readTree(
                        post(
                            "$base/commit",
                            """{"appends":[{"namespace":"ns","table":"target",
                    "files":[{"path":"s3://synthetic/data2","record_count":5,"file_size_bytes":500}]}]}""",
                        ).bodyAsText(),
                    )["snapshot_id"].asLong()
                val retiredAt = second + 1
                val catalogId =
                    db.jdbi.withHandle<Long, Exception> { h ->
                        h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'delete-test'")
                            .mapTo(Long::class.java).one()
                    }
                db.jdbi.useHandle<Exception> { h ->
                    h.createUpdate(
                        "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) " +
                            "VALUES (:cat, :snapshot, 0)",
                    ).bind("cat", catalogId).bind("snapshot", retiredAt).execute()
                    h.createUpdate("UPDATE hog_catalog SET last_snapshot_id = :snapshot WHERE catalog_id = :cat")
                        .bind("cat", catalogId).bind("snapshot", retiredAt).execute()
                    h.createUpdate(
                        "UPDATE hog_data_file SET end_snapshot = :snapshot " +
                            "WHERE catalog_id = :cat AND path = :path",
                    ).bind("cat", catalogId).bind("snapshot", retiredAt)
                        .bind("path", "s3://synthetic/data2").execute()
                }
                val fileId =
                    db.jdbi.withHandle<Long, Exception> { h ->
                        h.createQuery(
                            "SELECT data_file_id FROM hog_data_file WHERE catalog_id = :cat AND path = :path",
                        ).bind("cat", catalogId).bind("path", "s3://synthetic/data2")
                            .mapTo(Long::class.java).one()
                    }

                fun deleteAt(
                    readSnapshot: Long,
                    dv: String,
                ) = """{"idempotency_key":"${java.util.UUID.randomUUID()}","read_snapshot":$readSnapshot,
                    "deletes":[{"namespace":"ns","table":"target","expected_table_uuid":"$uuid","files":[
                    {"data_file_id":$fileId,"path":"s3://synthetic/$dv","delete_count":1,
                     "file_size_bytes":100}]}]}"""
                assertThat(post("$base/commit/deletes/prepared", deleteAt(second, "dv-retired")).status)
                    .describedAs("live at read_snapshot, retired since")
                    .isEqualTo(HttpStatusCode.Conflict)
                assertThat(post("$base/commit/mutations/prepared", deleteAt(second, "dv-retired-m")).status)
                    .describedAs("same rule on the mutation endpoint")
                    .isEqualTo(HttpStatusCode.Conflict)
                assertThat(post("$base/commit/deletes/prepared", deleteAt(retiredAt, "dv-dead")).status)
                    .describedAs("already dead at read_snapshot")
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

    /**
     * /commit/transaction at the WIRE, which nothing covered: the guarded
     * -targets validation the route enforces, an append+delete transaction
     * over two tables with a same-commit private delete reference, and the
     * conflict rule the route depends on — "unchanged" binds the tables
     * the transaction DELETES from, so an append-only target does not 409
     * on a concurrent insert.
     */
    @Test
    fun `transaction publishes multi-table DML and guards only its delete targets`() {
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
                val base = "/v1/catalogs/txn-test"
                val json = ObjectMapper()
                post("/v1/catalogs", """{"name":"txn-test","data_path":"s3://synthetic/"}""")
                post("$base/namespaces", """{"name":"ns"}""")

                fun uuidOf(body: String) = json.readTree(body)["table_uuid"].asText()
                val aUuid =
                    uuidOf(
                        post(
                            "$base/namespaces/ns/tables",
                            """{"name":"a","columns":[{"name":"id","type":"long"}]}""",
                        ).bodyAsText(),
                    )
                val bUuid =
                    uuidOf(
                        post(
                            "$base/namespaces/ns/tables",
                            """{"name":"b","columns":[{"name":"id","type":"long"}]}""",
                        ).bodyAsText(),
                    )
                val seeded =
                    json.readTree(
                        post(
                            "$base/commit",
                            """{"appends":[{"namespace":"ns","table":"a","files":[
                    {"path":"s3://synthetic/a-old","record_count":7,"file_size_bytes":700}]}]}""",
                        ).bodyAsText(),
                    )["snapshot_id"].asLong()

                assertThat(json.readTree(client.get(base).bodyAsText())["capabilities"].map { it.asText() })
                    .contains("atomic-dml-transactions-v1")

                // Guarded-targets validation: every one of these is a 422
                // before any work happens.
                val guardFailures =
                    listOf(
                        """{"read_snapshot":$seeded,"appends":[{"namespace":"ns","table":"b",
                    "expected_table_uuid":"$bUuid","files":[]}]}""",
                        """{"idempotency_key":"${java.util.UUID.randomUUID()}","appends":[{"namespace":"ns",
                    "table":"b","expected_table_uuid":"$bUuid","files":[]}]}""",
                        """{"idempotency_key":"${java.util.UUID.randomUUID()}","read_snapshot":$seeded,
                    "appends":[{"namespace":"ns","table":"b","files":[
                    {"path":"s3://synthetic/unguarded","record_count":1,"file_size_bytes":100}]}]}""",
                        """{"idempotency_key":"${java.util.UUID.randomUUID()}","read_snapshot":$seeded,
                    "deletes":[{"namespace":"ns","table":"a","files":[
                    {"data_file_id":1,"path":"s3://synthetic/dv-unguarded","delete_count":1,
                     "file_size_bytes":100}]}]}""",
                    )
                for (body in guardFailures) {
                    assertThat(post("$base/commit/transaction", body).status)
                        .describedAs(body)
                        .isEqualTo(HttpStatusCode.UnprocessableEntity)
                }
                assertThat(json.readTree(client.get(base).bodyAsText())["head_snapshot_id"].asLong())
                    .isEqualTo(seeded)

                // A concurrent INSERT into b, which the transaction below
                // only APPENDS to. Under the old rule this alone made every
                // append-only transaction on b a 409.
                post(
                    "$base/commit",
                    """{"appends":[{"namespace":"ns","table":"b","files":[
                    {"path":"s3://synthetic/b-concurrent","record_count":3,"file_size_bytes":300}]}]}""",
                )

                val key = java.util.UUID.randomUUID()
                val transaction = """{"idempotency_key":"$key","read_snapshot":$seeded,
                    "appends":[
                      {"namespace":"ns","table":"a","expected_table_uuid":"$aUuid","files":[
                        {"path":"s3://synthetic/a-new","record_count":4,"file_size_bytes":400}]},
                      {"namespace":"ns","table":"b","expected_table_uuid":"$bUuid","files":[
                        {"path":"s3://synthetic/b-new","record_count":5,"file_size_bytes":500}]}],
                    "deletes":[{"namespace":"ns","table":"a","expected_table_uuid":"$aUuid","files":[
                      {"data_file_id":1,"path":"s3://synthetic/dv-a","delete_count":2,"file_size_bytes":100},
                      {"data_file_id":0,"data_file_path":"s3://synthetic/a-new",
                       "path":"s3://synthetic/dv-staged","delete_count":1,"file_size_bytes":100}]}]}"""
                val result = post("$base/commit/transaction", transaction)
                assertThat(result.status).isEqualTo(HttpStatusCode.OK)
                val committed = json.readTree(result.bodyAsText())["snapshot_id"].asLong()
                assertThat(json.readTree(post("$base/commit/transaction", transaction).bodyAsText())["snapshot_id"])
                    .isEqualTo(json.readTree(result.bodyAsText())["snapshot_id"])
                assertThat(json.readTree(client.get("$base/commit/receipts/$key").bodyAsText())["snapshot_id"].asLong())
                    .isEqualTo(committed)

                // Both tables moved, in one snapshot, including the delete
                // against a file this very commit staged.
                assertThat(
                    json.readTree(client.get("$base/namespaces/ns/tables/a").bodyAsText())["record_count"].asLong(),
                ).isEqualTo(11)
                assertThat(
                    json.readTree(client.get("$base/namespaces/ns/tables/b").bodyAsText())["record_count"].asLong(),
                ).isEqualTo(8)
                db.jdbi.useHandle<Exception> { h ->
                    assertThat(
                        h.createQuery(
                            """
                            SELECT COUNT(*) FROM hog_delete_file d
                            JOIN hog_catalog c ON c.catalog_id = d.catalog_id
                            WHERE c.name = 'txn-test' AND d.begin_snapshot = :s AND d.end_snapshot IS NULL
                            """,
                        ).bind("s", committed).mapTo(Long::class.java).one(),
                    ).isEqualTo(2)
                }

                // The other half of the rule: an INSERT into the DELETE
                // target a since the read snapshot IS a conflict.
                post(
                    "$base/commit",
                    """{"appends":[{"namespace":"ns","table":"a","files":[
                    {"path":"s3://synthetic/a-concurrent","record_count":1,"file_size_bytes":100}]}]}""",
                )
                val stale = """{"idempotency_key":"${java.util.UUID.randomUUID()}","read_snapshot":$committed,
                    "appends":[{"namespace":"ns","table":"b","expected_table_uuid":"$bUuid","files":[
                      {"path":"s3://synthetic/b-later","record_count":1,"file_size_bytes":100}]}],
                    "deletes":[{"namespace":"ns","table":"a","expected_table_uuid":"$aUuid","files":[]}]}"""
                assertThat(post("$base/commit/transaction", stale).status).isEqualTo(HttpStatusCode.Conflict)

                // Append-only on the same read snapshot, with a concurrent
                // insert into its own target, still publishes.
                val appendOnly = """{"idempotency_key":"${java.util.UUID.randomUUID()}",
                    "read_snapshot":$committed,
                    "appends":[{"namespace":"ns","table":"a","expected_table_uuid":"$aUuid","files":[
                      {"path":"s3://synthetic/a-append-only","record_count":2,"file_size_bytes":200}]}]}"""
                assertThat(post("$base/commit/transaction", appendOnly).status).isEqualTo(HttpStatusCode.OK)
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
