package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
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
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Filtering GET .../tables/{table}/files by partition, and the
 * /partitions/values endpoint that feeds the dropdown it hangs off of.
 *
 * The values are the stored, transformed strings (a day ordinal, an
 * identity value) — the match is plain string equality, so the server
 * never encodes. Seeded: a table partitioned by (day, identity), with
 * files spread over two days and two identity values.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionFilterApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalog = "pf"
    private val filesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables/t/files"
    private val valuesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables/t/partitions/values"

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
            client.postJson(
                "/v1/catalogs/$catalog/namespaces/ns/tables",
                """{"name": "t", "columns": [{"name": "day", "type": "string"}, {"name": "uid", "type": "string"}]}""",
            )
            seedSpec()
            // Files over two days (key 0) and two identity values (key 1).
            // Day values are ordinals on the wire (as the writer would send).
            val files =
                listOf(
                    Triple("f1", "20697", "u1"),
                    Triple("f2", "20697", "u2"),
                    Triple("f3", "20698", "u1"),
                    Triple("f4", "20698", "u2"),
                    Triple("f5", "20698", "u2"),
                )
            files.forEachIndexed { i, (path, day, uid) ->
                val res =
                    client.postJson(
                        "/v1/catalogs/$catalog/commit",
                        """{"appends": [{"namespace": "ns", "table": "t", "files": [
                           {"path": "s3://b/$catalog/$path.parquet", "record_count": 10,
                            "file_size_bytes": ${1000 + i},
                            "partition_values": ["$day", "$uid"]}]}]}""",
                    )
                assertThat(res.status).describedAs(res.bodyAsText()).isEqualTo(HttpStatusCode.OK)
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

    private fun seedSpec() {
        db.jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                    .bind(0, catalog).mapTo(Long::class.java).one()
            val tableId =
                h.createQuery(
                    """
                    SELECT table_id FROM hog_table_version
                     WHERE catalog_id = ? AND name = ? AND end_snapshot IS NULL
                    """,
                ).bind(0, catalogId).bind(1, "t").mapTo(Long::class.java).one()
            val fieldIds =
                h.createQuery(
                    """
                    SELECT field_id FROM hog_column
                     WHERE catalog_id = ? AND table_id = ? AND end_snapshot IS NULL
                     ORDER BY ordinal
                    """,
                ).bind(0, catalogId).bind(1, tableId).mapTo(Long::class.java).list()
            h.createUpdate(
                """
                INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot)
                VALUES (?, ?, 1, 0)
                """,
            ).bind(0, catalogId).bind(1, tableId).execute()
            h.createUpdate(
                """
                INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index,
                                                 source_field_id, transform)
                VALUES (:c, :t, 1, 0, :f0, 'day'), (:c, :t, 1, 1, :f1, 'identity')
                """,
            ).bind("c", catalogId).bind("t", tableId)
                .bind("f0", fieldIds[0]).bind("f1", fieldIds[1]).execute()
        }
    }

    private suspend fun paths(response: HttpResponse): List<String> =
        body(response).map { it["path"].asText().substringAfterLast("/") }

    // -- the filter ----------------------------------------------------------

    @Test
    fun `no partition param returns every file`() =
        api { client ->
            assertThat(paths(client.get(filesUrl))).hasSize(5)
        }

    @Test
    fun `filter by one key narrows to that partition`() =
        api { client ->
            // day 20698 → f3, f4, f5.
            val got = paths(client.get("$filesUrl?partition=0:20698"))
            assertThat(got).containsExactlyInAnyOrder("f3.parquet", "f4.parquet", "f5.parquet")
        }

    @Test
    fun `filter by two keys ANDs them`() =
        api { client ->
            // day 20698 AND uid u2 → f4, f5.
            val got = paths(client.get("$filesUrl?partition=0:20698&partition=1:u2"))
            assertThat(got).containsExactlyInAnyOrder("f4.parquet", "f5.parquet")
        }

    @Test
    fun `a value no file has returns an empty page, not an error`() =
        api { client ->
            val got = paths(client.get("$filesUrl?partition=1:nobody"))
            assertThat(got).isEmpty()
        }

    @Test
    fun `the filter composes with paging and sort`() =
        api { client ->
            // Within day 20698, page by size.
            val page =
                paths(client.get("$filesUrl?partition=0:20698&sort=size&order=asc&limit=2"))
            assertThat(page).hasSize(2)
            // The filter narrows before paging: the 3 day-20698 files page as 2+1.
            val page2 =
                paths(client.get("$filesUrl?partition=0:20698&sort=size&order=asc&limit=2&offset=2"))
            assertThat(page2).hasSize(1)
        }

    @Test
    fun `a malformed partition param or out-of-range key is refused`() =
        api { client ->
            assertThat(client.get("$filesUrl?partition=notanumber").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$filesUrl?partition=abc:20698").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            // key_index 5 doesn't exist on a 2-field spec.
            assertThat(client.get("$filesUrl?partition=5:20698").status)
                .isEqualTo(HttpStatusCode.UnprocessableEntity)
        }

    // -- the distinct-values endpoint ----------------------------------------

    @Test
    fun `partition values returns the distinct stored values per field`() =
        api { client ->
            val body = body(client.get(valuesUrl))
            assertThat(body["spec_id"].asLong()).isEqualTo(1)
            val fields = body["fields"]
            assertThat(fields).hasSize(2)

            val day = fields[0]
            assertThat(day["transform"].asText()).isEqualTo("day")
            val dayValues = day["values"].map { it.asText() }
            assertThat(dayValues).containsExactlyInAnyOrder("20697", "20698")

            val uid = fields[1]
            assertThat(uid["transform"].asText()).isEqualTo("identity")
            val uidValues = uid["values"].map { it.asText() }
            assertThat(uidValues).containsExactlyInAnyOrder("u1", "u2")
        }

    @Test
    fun `the values endpoint feeds a value that the filter then matches`() =
        api { client ->
            // Round-trip: read a stored day value from /partitions/values and
            // filter by it verbatim — the path a dropdown-driven UI takes.
            val fields = body(client.get(valuesUrl))["fields"]
            val aDay = fields[0]["values"][0].asText()
            val got = paths(client.get("$filesUrl?partition=0:$aDay"))
            assertThat(got).isNotEmpty()
            assertThat(got).allMatch {
                it in
                    listOf("f1.parquet", "f2.parquet", "f3.parquet", "f4.parquet", "f5.parquet")
            }
        }
}
