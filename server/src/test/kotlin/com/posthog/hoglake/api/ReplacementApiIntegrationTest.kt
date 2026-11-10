package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

/** `POST /commit/replacements/prepared` through the real application on PostgreSQL. */
@Tag("integration")
class ReplacementApiIntegrationTest {
    private val json = ObjectMapper()

    @Test
    fun `the capability is advertised`() {
        PgTestSupport.freshDatabase().use { db ->
            CatalogService(db.jdbi).createCatalog("caps", "s3://synthetic/caps")
            testApplication {
                application { App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi).module(this) }
                val body = json.readTree(client.get("/v1/catalogs/caps").bodyAsText())
                assertThat(body["capabilities"].map { it.asText() }).contains("atomic-partition-replacement-v1")
            }
        }
    }

    @Test
    fun `a replacement ends a day, replays from its receipt and answers each refusal with its status`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            catalogs.createCatalog("repl", "s3://synthetic/repl")
            catalogs.createNamespace("repl", "ns")
            catalogs.createTable("repl", "ns", "ev", listOf(ColumnDef("day", ColType.LONG)))
            AlterService(db.jdbi).alterTable(
                "repl",
                "ns",
                "ev",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
            )
            val uuid = catalogs.getTable("repl", "ns", "ev").tableUuid
            testApplication {
                application { App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi).module(this) }
                val base = "/v1/catalogs/repl"

                suspend fun post(
                    path: String,
                    body: String,
                ): HttpResponse =
                    client.post("$base$path") {
                        contentType(ContentType.Application.Json)
                        setBody(body)
                    }

                suspend fun tree(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

                fun file(
                    name: String,
                    day: String,
                ) = """{"path":"s3://synthetic/repl/$name","record_count":5,"file_size_bytes":100,"footer_size":20,""" +
                    """"partition_values":["$day"]}"""

                suspend fun head() = tree(client.get(base))["head_snapshot_id"].asLong()

                val seeded =
                    post(
                        "/commit",
                        """{"appends":[{"namespace":"ns","table":"ev","files":[${file("a", "20401")},""" +
                            """${file("b", "20402")}]}],"read_snapshot":${head()}}""",
                    )
                assertThat(seeded.status).isEqualTo(HttpStatusCode.OK)
                // Other endpoints never report ended_files.
                assertThat(tree(seeded).has("ended_files")).isFalse()

                val key = UUID.randomUUID()

                fun request(
                    read: Long,
                    extra: String = "",
                    ends: String =
                        """[{"namespace":"ns","table":"ev","expected_table_uuid":"$uuid",""" +
                            """"partitions":[["20401"]]}]""",
                ) = """{"idempotency_key":"$key","read_snapshot":$read,"author":"t","message":"m",""" +
                    """"appends":[{"namespace":"ns","table":"ev","files":[${file("c", "20401")}]}],""" +
                    """"file_ends":$ends$extra}"""

                val read = head()
                val replaced = post("/commit/replacements/prepared", request(read))
                assertThat(replaced.status).isEqualTo(HttpStatusCode.OK)
                val result = tree(replaced)
                assertThat(result["snapshot_id"].asLong()).isEqualTo(read + 1)
                assertThat(result["ended_files"].asLong()).isEqualTo(1)

                // A replay returns the original snapshot, and the receipt serves it.
                val replay = tree(post("/commit/replacements/prepared", request(read)))
                assertThat(replay["snapshot_id"].asLong()).isEqualTo(read + 1)
                assertThat(replay.has("ended_files")).isFalse()
                assertThat(head()).isEqualTo(read + 1)
                assertThat(tree(client.get("$base/commit/receipts/$key"))["snapshot_id"].asLong()).isEqualTo(read + 1)

                // The old routes refuse the new field rather than ignoring it.
                val unknown = post("/commit/prepared", request(read))
                assertThat(unknown.status).isEqualTo(HttpStatusCode.BadRequest)

                // 400: deletes and require_unchanged_tables.
                val fresh = head()
                val withDeletes =
                    request(fresh, ""","deletes":[{"namespace":"ns","table":"ev","files":[]}]""")
                        .replace(key.toString(), UUID.randomUUID().toString())
                assertThat(post("/commit/replacements/prepared", withDeletes).status)
                    .isEqualTo(HttpStatusCode.BadRequest)
                val withGuard =
                    request(fresh, ""","require_unchanged_tables":true""")
                        .replace(key.toString(), UUID.randomUUID().toString())
                assertThat(post("/commit/replacements/prepared", withGuard).status)
                    .isEqualTo(HttpStatusCode.BadRequest)

                // 422: no author, no message.
                val newKey = UUID.randomUUID().toString()
                val anonymous = request(fresh).replace(key.toString(), newKey).replace(""""author":"t",""", "")
                assertThat(post("/commit/replacements/prepared", anonymous).status)
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
                val silent = request(fresh).replace(key.toString(), newKey).replace(""""message":"m",""", "")
                assertThat(post("/commit/replacements/prepared", silent).status)
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
                // 422: no file_ends at all.
                val bare =
                    """{"idempotency_key":"$newKey","read_snapshot":$fresh,"author":"t","message":"m"}"""
                assertThat(post("/commit/replacements/prepared", bare).status)
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)

                // 409 table_recreated for a wrong expected_table_uuid, with the re-prepare body.
                val wrong =
                    request(
                        fresh,
                        ends =
                            """[{"namespace":"ns","table":"ev","expected_table_uuid":"${UUID.randomUUID()}",""" +
                                """"all_files":true}]""",
                    ).replace(key.toString(), UUID.randomUUID().toString())
                val recreated = post("/commit/replacements/prepared", wrong)
                assertThat(recreated.status).isEqualTo(HttpStatusCode.Conflict)
                assertThat(tree(recreated)["error"].asText()).isEqualTo("table_recreated")
            }
        }
    }

    @Test
    fun `a spec mismatch is a 409 with its own code`() {
        val (status, body) = errorBody(HoglakeException.PartitionSpecMismatch("why"))
        assertThat(status).isEqualTo(HttpStatusCode.Conflict)
        assertThat(body.error).isEqualTo("partition_spec_mismatch")
    }
}
