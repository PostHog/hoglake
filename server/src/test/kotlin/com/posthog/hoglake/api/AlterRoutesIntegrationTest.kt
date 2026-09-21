package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * Wire-level tests for POST .../alter: raw JSON against the OpenAPI
 * contract (snake_case fields, spec status codes), with the same
 * serialization stack App.module installs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlterRoutesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val json = ObjectMapper()
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    // ---- harness ---------------------------------------------------------

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                // App.module's serialization + error stack, by sharing
                // its definition rather than restating it (api/WireJson.kt).
                this.install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                this.install(StatusPages) { installErrorMapping() }
                installAlterRoutes(alter)
            }
            block(client)
        }

    private suspend fun HttpClient.alterJson(
        path: String,
        body: String,
    ): HttpResponse =
        post(path) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    private suspend fun assertApiError(
        response: HttpResponse,
        status: HttpStatusCode,
        code: String,
    ): JsonNode {
        assertThat(response.status).isEqualTo(status)
        assertThat(response.contentType()?.contentSubtype).isEqualTo("json")
        val node = body(response)
        assertThat(node["error"].asText()).isEqualTo(code)
        return node
    }

    /** Catalog + namespace + a two-column table; returns the alter URL. */
    private fun fixture(): String {
        val cat = "alter-wire-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "events",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("ts", ColType.TIMESTAMPTZ),
            ),
        )
        return "/v1/catalogs/$cat/namespaces/ns/tables/events/alter"
    }

    // ---- happy path ------------------------------------------------------

    @Test
    fun `alter happy path - add column and set spec, snake_case wire shape`() =
        api { client ->
            val url = fixture()
            val resp =
                client.alterJson(
                    url,
                    """
            {"ops": [
                {"op": "add_column",
                 "column": {"name": "team_id", "type": "int", "nullable": false}},
                {"op": "set_partition_spec",
                 "fields": [
                    {"source_field_id": 2, "transform": "day"},
                    {"source_field_id": 3, "transform": "bucket", "transform_param": 8}
                 ]}
            ]}
            """,
                )
            assertThat(resp.status).isEqualTo(HttpStatusCode.OK)
            val table = body(resp)
            assertThat(table["name"].asText()).isEqualTo("events")
            assertThat(table["namespace"].asText()).isEqualTo("ns")
            assertThat(table["table_uuid"].asText()).isNotBlank()
            // An alter is a DDL commit: the response names the snapshot it
            // just made (#35), so a post-alter read can pin to it.
            assertThat(table["snapshot_id"].asLong()).isPositive()
            assertThat(table["record_count"].asLong()).isEqualTo(0)
            assertThat(table["file_count"].asLong()).isEqualTo(0)
            assertThat(table["file_size_bytes"].asLong()).isEqualTo(0)

            val cols = table["columns"]
            assertThat(cols.map { it["name"].asText() }).containsExactly("id", "ts", "team_id")
            val added = cols[2]
            assertThat(added["type"].asText()).isEqualTo("int")
            assertThat(added["nullable"].asBoolean()).isFalse()
            assertThat(added["field_id"].asLong()).isEqualTo(3)
            assertThat(added["ordinal"].asInt()).isEqualTo(2)

            val spec = table["partition_spec"]
            assertThat(spec["spec_id"].asLong()).isEqualTo(1)
            val fields = spec["fields"]
            assertThat(fields).hasSize(2)
            assertThat(fields[0]["source_field_id"].asLong()).isEqualTo(2)
            assertThat(fields[0]["transform"].asText()).isEqualTo("day")
            assertThat(fields[0].has("transform_param")).isFalse()
            assertThat(fields[1]["transform"].asText()).isEqualTo("bucket")
            assertThat(fields[1]["transform_param"].asInt()).isEqualTo(8)
        }

    @Test
    fun `unpartitioned table omits partition_spec entirely`() =
        api { client ->
            val url = fixture()
            val resp =
                client.alterJson(
                    url,
                    """{"ops": [{"op": "rename_column", "from": "ts", "to": "occurred_at"}]}""",
                )
            assertThat(resp.status).isEqualTo(HttpStatusCode.OK)
            val table = body(resp)
            assertThat(table.has("partition_spec")).isFalse()
            assertThat(table["columns"].map { it["name"].asText() })
                .containsExactly("id", "occurred_at")
        }

    @Test
    fun `alter returns its own snapshot - the new head, and the next alter's is one later`() =
        api { client ->
            val url = fixture()
            val cat = url.split("/")[3]

            // The fixture is at head 3 (catalog, namespace, table). The
            // alter's snapshot_id must be the NEW head, not just any
            // positive number — this is the value a client pins reads to.
            val headBefore = catalogs.getCatalog(cat).headSnapshotId
            val first = body(client.alterJson(url, """{"ops": [{"op": "rename_column", "from": "ts", "to": "a"}]}"""))
            assertThat(first["snapshot_id"].asLong()).isEqualTo(headBefore + 1)
            assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(headBefore + 1)

            // A second alter commits its own snapshot, exactly one later —
            // not the first's, not a stale head.
            val second = body(client.alterJson(url, """{"ops": [{"op": "rename_column", "from": "a", "to": "b"}]}"""))
            assertThat(second["snapshot_id"].asLong()).isEqualTo(headBefore + 2)
        }

    @Test
    fun `alter snapshot_id is exact above 2^53`() =
        api { client ->
            // snapshot_id is an int64 on the wire; it must round-trip as an
            // exact integer, not a Number-rounded one. Drive head to just
            // under 2^53 and alter once, so the returned snapshot is
            // 2^53 + 1 — the smallest value a JS Number cannot represent
            // (its even neighbour 2^53 + 2 is representable, so this pins
            // exactness rather than magnitude).
            val url = fixture()
            val cat = url.split("/")[3]
            val big = 9007199254740992L // 2^53
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_catalog SET last_snapshot_id = :big WHERE name = :cat",
                )
                    .bind("big", big)
                    .bind("cat", cat)
                    .execute()
            }
            val altered = body(client.alterJson(url, """{"ops": [{"op": "rename_column", "from": "ts", "to": "a"}]}"""))
            // 2^53 + 1 as text; asLong() would silently read the rounded
            // double. The raw node must be integral and exact.
            assertThat(altered["snapshot_id"].isIntegralNumber).isTrue()
            assertThat(altered["snapshot_id"].asText()).isEqualTo("9007199254740993")
        }

    // ---- error shapes ----------------------------------------------------

    @Test
    fun `unknown op is 400 bad_request`() =
        api { client ->
            val url = fixture()
            val resp = client.alterJson(url, """{"ops": [{"op": "explode_table"}]}""")
            val err = assertApiError(resp, HttpStatusCode.BadRequest, "bad_request")
            assertThat(err["detail"].asText()).contains("explode_table")
        }

    @Test
    fun `missing field for the chosen op is 400 bad_request`() =
        api { client ->
            val url = fixture()
            val cases =
                listOf(
                    """{"ops": [{"op": "add_column"}]}""",
                    """{"ops": [{"op": "drop_column"}]}""",
                    """{"ops": [{"op": "rename_column", "from": "id"}]}""",
                    """{"ops": [{"op": "rename_table"}]}""",
                    """{"ops": [{"op": "set_partition_spec"}]}""",
                )
            for (case in cases) {
                assertApiError(client.alterJson(url, case), HttpStatusCode.BadRequest, "bad_request")
            }
        }

    @Test
    fun `empty ops is 400 bad_request`() =
        api { client ->
            val url = fixture()
            assertApiError(
                client.alterJson(url, """{"ops": []}"""),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
        }

    @Test
    fun `semantic failure is 422 validation`() =
        api { client ->
            val url = fixture()
            val resp = client.alterJson(url, """{"ops": [{"op": "drop_column", "name": "nope"}]}""")
            val err = assertApiError(resp, HttpStatusCode.UnprocessableEntity, "validation")
            assertThat(err["detail"].asText()).contains("nope")

            // Illegal promotion.
            assertApiError(
                client.alterJson(
                    url,
                    """{"ops": [{"op": "promote_column", "name": "id", "to": "int"}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }

    @Test
    fun `unknown table is 404 not_found`() =
        api { client ->
            fixture()
            val resp =
                client.alterJson(
                    "/v1/catalogs/no-such/namespaces/ns/tables/events/alter",
                    """{"ops": [{"op": "drop_column", "name": "ts"}]}""",
                )
            assertApiError(resp, HttpStatusCode.NotFound, "not_found")
        }

    @Test
    fun `rename_table collision is 409 already_exists`() =
        api { client ->
            val url = fixture()
            val cat = url.removePrefix("/v1/catalogs/").substringBefore("/")
            catalogs.createTable(cat, "ns", "taken", listOf(ColumnDef("id", ColType.LONG)))
            val resp =
                client.alterJson(
                    url,
                    """{"ops": [{"op": "rename_table", "new_name": "taken"}]}""",
                )
            assertApiError(resp, HttpStatusCode.Conflict, "already_exists")
        }
}
