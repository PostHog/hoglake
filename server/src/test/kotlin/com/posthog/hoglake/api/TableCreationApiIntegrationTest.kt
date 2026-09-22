package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.service.CatalogService
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
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableCreationApiIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
    private val catalogs = CatalogService(db.jdbi)
    private val json = ObjectMapper()
    private val definition = """{"namespace":"test","name":"target","columns":[{"name":"id","type":"long"}]}"""

    @AfterAll
    fun close() = db.close()

    private fun api(block: suspend (HttpClient, String) -> Unit) =
        testApplication {
            val catalog = "wire-" + UUID.randomUUID().toString().replace("-", "")
            catalogs.createCatalog(catalog, "s3://bucket/$catalog")
            catalogs.createNamespace(catalog, "test")
            application { app.module(this) }
            block(client, "/v1/catalogs/$catalog")
        }

    private suspend fun HttpClient.prepare(
        path: String,
        body: String = definition,
    ) = put(path) {
        contentType(ContentType.Application.Json)
        setBody(body)
    }

    private suspend fun HttpClient.publish(
        path: String,
        files: List<Map<String, Any?>> = emptyList(),
    ) = post("$path/commit") {
        contentType(ContentType.Application.Json)
        setBody(json.writeValueAsString(mapOf("files" to files)))
    }

    private suspend fun error(
        response: HttpResponse,
        status: HttpStatusCode,
        code: String,
    ) {
        assertThat(response.status).isEqualTo(status)
        val body = json.readTree(response.bodyAsText())
        assertThat(body["error"].asText()).isEqualTo(code)
        assertThat(body["detail"].asText()).isNotBlank()
    }

    @Test
    fun `sorted preparation rejects ordinary and partition-only endpoints`() =
        api { client, base ->
            val path = "$base/table-creations/${UUID.randomUUID()}"
            val request = json.readTree(definition) as com.fasterxml.jackson.databind.node.ObjectNode
            request.set<com.fasterxml.jackson.databind.JsonNode>(
                "sort_fields",
                json.readTree("""[{"source_field_id":1,"direction":"desc","null_order":"nulls_first"}]"""),
            )
            assertThat(client.prepare(path, request.toString()).status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(
                client.prepare("$path/partitioned", request.toString()).status,
            ).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(client.prepare("$path/sorted", request.toString()).status).isEqualTo(HttpStatusCode.OK)
            assertThat(client.publish(path).status).isEqualTo(HttpStatusCode.OK)
            val table = json.readTree(client.get("$base/namespaces/test/tables/target").bodyAsText())
            assertThat(table["sort_spec"]["fields"][0]["direction"].asText()).isEqualTo("desc")
        }

    @Test
    fun `partition preparation requires its distinct endpoint`() =
        api { client, base ->
            val path = "$base/table-creations/${UUID.randomUUID()}"
            val request = json.readTree(definition) as com.fasterxml.jackson.databind.node.ObjectNode
            request.set<com.fasterxml.jackson.databind.JsonNode>(
                "partition_fields",
                json.readTree("""[{"source_field_id":1,"transform":"identity"}]"""),
            )
            assertThat(client.prepare(path, request.toString()).status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(client.prepare("$path/partitioned", request.toString()).status).isEqualTo(HttpStatusCode.OK)
            assertThat(client.publish(path).status).isEqualTo(HttpStatusCode.OK)
            val table = json.readTree(client.get("$base/namespaces/test/tables/target").bodyAsText())
            assertThat(table["partition_spec"]["fields"][0]["source_field_id"].asLong()).isEqualTo(1)
        }

    @Test
    fun `wire lifecycle preserves identities and terminal receipts`() =
        api { client, base ->
            val capabilities = json.readTree(client.get(base).bodyAsText())["capabilities"]
            assertThat(capabilities.map { it.asText() }).contains("atomic-table-creation-v1")
            val id = UUID.randomUUID()
            val path = "$base/table-creations/$id"
            val response = client.prepare(path)
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val prepared = json.readTree(response.bodyAsText())
            assertThat(prepared["state"].asText()).isEqualTo("prepared")
            assertThat(prepared["operation_id"].asText()).isEqualTo(id.toString())
            assertThat(prepared["columns"][0]["field_id"].asLong()).isEqualTo(1)
            assertThat(prepared["columns"][0]["type"].asText()).isEqualTo("long")
            assertThat(prepared["write_path"].asText()).contains(prepared["table_uuid"].asText())
            assertThat(json.readTree(client.prepare(path).bodyAsText())).isEqualTo(prepared)
            assertThat(json.readTree(client.get(path).bodyAsText())).isEqualTo(prepared)
            error(
                client.prepare(path, definition.replace("target", "different")),
                HttpStatusCode.Conflict,
                "commit_conflict",
            )
            val committed = json.readTree(client.publish(path).bodyAsText())
            assertThat(committed["state"].asText()).isEqualTo("committed")
            assertThat(committed["snapshot_id"].asLong()).isPositive()
            assertThat(committed["table_uuid"]).isEqualTo(prepared["table_uuid"])
            assertThat(json.readTree(client.publish(path).bodyAsText())).isEqualTo(committed)
            assertThat(json.readTree(client.post("$path/abort").bodyAsText())).isEqualTo(committed)
            val second = "$base/table-creations/${UUID.randomUUID()}"
            client.prepare(second)
            val rejected = json.readTree(client.publish(second).bodyAsText())
            assertThat(rejected["state"].asText()).isEqualTo("rejected")
            assertThat(rejected["reason"].asText()).isEqualTo("target_exists")
            val third = "$base/table-creations/${UUID.randomUUID()}"
            client.prepare(third)
            assertThat(json.readTree(client.post("$third/abort").bodyAsText())["state"].asText()).isEqualTo("aborted")
            assertThat(json.readTree(client.publish(third).bodyAsText())["state"].asText()).isEqualTo("aborted")
        }

    @Test
    fun `malformed UUID missing operation and malformed body have structured errors`() =
        api { client, base ->
            val path = "$base/table-creations/not-a-uuid"
            error(client.prepare(path), HttpStatusCode.BadRequest, "bad_request")
            error(client.get(path), HttpStatusCode.BadRequest, "bad_request")
            error(client.publish(path), HttpStatusCode.BadRequest, "bad_request")
            error(client.post("$path/abort"), HttpStatusCode.BadRequest, "bad_request")
            error(client.get("$base/table-creations/${UUID.randomUUID()}"), HttpStatusCode.NotFound, "not_found")
            error(
                client.prepare("$base/table-creations/${UUID.randomUUID()}", "{"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
        }

    @Test
    fun `invalid publication never becomes visible and can be corrected`() =
        api { client, base ->
            val path = "$base/table-creations/${UUID.randomUUID()}"
            val prepared = json.readTree(client.prepare(path).bodyAsText())
            val prefix = prepared["write_path"].asText()
            val valid =
                mapOf<String, Any?>(
                    "path" to prefix + "part.parquet",
                    "record_count" to 1,
                    "file_size_bytes" to 100,
                    "footer_size" to 20,
                )
            val invalid =
                listOf(
                    listOf(valid + ("path" to "s3://bucket/other/part.parquet")),
                    listOf(valid + ("path" to prefix + "../other/part.parquet")),
                    listOf(valid + ("footer_size" to -1)),
                    listOf(valid + ("footer_size" to 93)),
                    listOf(valid + ("footer_size" to null)),
                    listOf(valid, valid),
                    List(10001) { valid + ("path" to prefix + "$it.parquet") },
                )
            for (files in invalid) {
                error(client.publish(path, files), HttpStatusCode.UnprocessableEntity, "validation")
                assertThat(json.readTree(client.get(path).bodyAsText())["state"].asText()).isEqualTo("prepared")
                error(client.get("$base/namespaces/test/tables/target"), HttpStatusCode.NotFound, "not_found")
            }
            val excessive =
                json.writeValueAsString(
                    mapOf(
                        "namespace" to "test",
                        "name" to "wide",
                        "columns" to
                            List(10001) { mapOf("name" to "c$it", "type" to "long") },
                    ),
                )
            error(
                client.prepare("$base/table-creations/${UUID.randomUUID()}", excessive),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            assertThat(client.publish(path, listOf(valid)).status).isEqualTo(HttpStatusCode.OK)
            error(client.publish(path), HttpStatusCode.Conflict, "commit_conflict")
        }
}
