package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.IcebergSingleValue
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
import java.util.Base64

/**
 * `ordering_bounds` on GET .../tables/{table}/files: the range each file
 * covers along the key its table is ORDERED by — the span a reader
 * prunes on, and the span that says whether two files overlap.
 *
 * The listing carries it so a page of files costs ONE stats read rather
 * than one per row; the per-file stats endpoint (FileStatsApiTest) still
 * owns every column's statistics. The corners are where the two keys and
 * the three absence cases meet:
 *
 *  - a SORTED table reports its leading sort field's stored bounds,
 *    decoded by the same BoundWire path the stats endpoint uses, with
 *    `field_id` naming the field. The trailing sort fields are not
 *    reported: a tiebreaker's per-file range spans the column and prunes
 *    nothing;
 *  - an UNSORTED table reports the row-id span, `field_id` absent (the
 *    row id is not a catalog column), and reports it for pending and
 *    failed files too — row_id_start is assigned at commit, so the span
 *    needs no statistics;
 *  - a COMPACTION OUTPUT of an unsorted table states its minimum and
 *    leaves its maximum JSON null: explicit row ids make
 *    `row_id_start + record_count - 1` arithmetic on a meaning the file
 *    does not have, and `_hog_row_id` is not a catalog column, so there
 *    is no statistics row to read a real maximum out of;
 *  - a sorted table's file with NO stats row for the key carries no
 *    `ordering_bounds` at all, which is a different fact from a null
 *    bound inside one.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileOrderingBoundsApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalog = "ordering"
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

    private fun enc(
        type: ColType,
        value: Any,
    ): String = Base64.getEncoder().encodeToString(IcebergSingleValue.encode(type, value))

    /** A table with id(long) and name(string); field ids 1 and 2. */
    private suspend fun createTable(
        client: HttpClient,
        name: String,
    ) {
        val response =
            client.postJson(
                tablesUrl,
                """{"name": "$name", "columns": [
                   {"name": "id", "type": "long"}, {"name": "name", "type": "string"}]}""",
            )
        assertThat(response.status).describedAs(response.bodyAsText()).isEqualTo(HttpStatusCode.Created)
    }

    private suspend fun commitFile(
        client: HttpClient,
        table: String,
        path: String,
        recordCount: Long,
        statsJson: String?,
    ) {
        val stats = statsJson?.let { ""","column_stats": [$it]""" } ?: ""
        val response =
            client.postJson(
                "/v1/catalogs/$catalog/commit",
                """{"appends": [{"namespace": "ns", "table": "$table", "files": [
                   {"path": "$path", "record_count": $recordCount, "file_size_bytes": 1024$stats}]}]}""",
            )
        assertThat(response.status).describedAs(response.bodyAsText()).isEqualTo(HttpStatusCode.OK)
    }

    private suspend fun files(
        client: HttpClient,
        table: String,
    ): JsonNode = body(client.get("$tablesUrl/$table/files"))

    private fun JsonNode.file(path: String): JsonNode = first { it["path"].asText() == path }

    /**
     * Turn a committed file into a compaction OUTPUT the only way a test
     * can without running a compaction: flip the flag the rewriter sets.
     * The flag is the whole contract — it is what tells every reader that
     * row_id_start has lost its positional meaning.
     */
    private fun markExplicitRowIds(path: String) =
        db.jdbi.withHandleUnchecked { h ->
            val updated =
                h.createUpdate("UPDATE hog_data_file SET explicit_row_ids = true WHERE path = :path")
                    .bind("path", path)
                    .execute()
            assertThat(updated).isEqualTo(1)
        }

    @Test
    fun `an unsorted table reports the row-id span, statistics or not`() =
        api { client ->
            createTable(client, "unsorted")
            commitFile(client, "unsorted", "s3://b/$catalog/unsorted/f1.parquet", 100, null)
            commitFile(client, "unsorted", "s3://b/$catalog/unsorted/f2.parquet", 50, null)

            val listing = files(client, "unsorted")
            val first = listing.file("s3://b/$catalog/unsorted/f1.parquet")["ordering_bounds"]
            // No field_id: the row id is the implicit ordering key and is
            // not a catalog column, so there is none to give.
            assertThat(first.has("field_id")).isFalse()
            assertThat(first["lower_bound"].asLong()).isEqualTo(0)
            assertThat(first["upper_bound"].asLong()).isEqualTo(99)

            // The second file's ids continue where the first's stopped —
            // and both files committed with NO column stats at all, which
            // the row-id span does not need.
            val second = listing.file("s3://b/$catalog/unsorted/f2.parquet")["ordering_bounds"]
            assertThat(second["lower_bound"].asLong()).isEqualTo(100)
            assertThat(second["upper_bound"].asLong()).isEqualTo(149)
        }

    @Test
    fun `a compaction output states its minimum and leaves its maximum null`() =
        api { client ->
            createTable(client, "compacted")
            val path = "s3://b/$catalog/compacted/out.parquet"
            commitFile(client, "compacted", path, 100, null)
            markExplicitRowIds(path)

            val bounds = files(client, "compacted").file(path)["ordering_bounds"]
            // row_id_start is min(input row ids) and still true as a lower
            // bound; the maximum it would imply is not.
            assertThat(bounds["lower_bound"].asLong()).isEqualTo(0)
            assertThat(bounds["upper_bound"].isNull).isTrue()
        }

    @Test
    fun `a sorted table reports its leading sort field's decoded bounds`() =
        api { client ->
            createTable(client, "sorted")
            // Sorted by id, tie-broken by name. Only `id` is reported.
            val altered =
                client.postJson(
                    "$tablesUrl/sorted/alter",
                    """{"ops": [{"op": "set_sort_order", "sort_fields": [
                       {"source_field_id": 1, "direction": "asc", "null_order": "nulls_last"},
                       {"source_field_id": 2, "direction": "asc", "null_order": "nulls_last"}]}]}""",
                )
            assertThat(altered.status).describedAs(altered.bodyAsText()).isEqualTo(HttpStatusCode.OK)

            val path = "s3://b/$catalog/sorted/f1.parquet"
            commitFile(
                client,
                "sorted",
                path,
                100,
                """{"field_id": 1, "value_count": 100, "null_count": 0,
                    "lower_bound": "${enc(ColType.LONG, 7L)}",
                    "upper_bound": "${enc(ColType.LONG, 9007199254740993L)}"},
                   {"field_id": 2, "value_count": 100, "null_count": 0,
                    "lower_bound": "${enc(ColType.STRING, "aardvark")}",
                    "upper_bound": "${enc(ColType.STRING, "zebra")}"}""",
            )

            val bounds = files(client, "sorted").file(path)["ordering_bounds"]
            assertThat(bounds["field_id"].asLong()).isEqualTo(1)
            assertThat(bounds["lower_bound"].asLong()).isEqualTo(7)
            // 2^53 + 1, exact: the decode path is BoundWire's, so the
            // listing inherits the same never-through-a-double rule the
            // per-file stats endpoint has.
            assertThat(bounds["upper_bound"].asLong()).isEqualTo(9007199254740993L)
            // The row id is NOT offered beside it: on a sorted table it
            // describes nothing a reader can use.
            assertThat(bounds.has("row_id_start")).isFalse()
        }

    @Test
    fun `a sorted table's file with no stats row for the key has no bounds at all`() =
        api { client ->
            createTable(client, "sortedpending")
            val altered =
                client.postJson(
                    "$tablesUrl/sortedpending/alter",
                    """{"ops": [{"op": "set_sort_order", "sort_fields": [
                       {"source_field_id": 1, "direction": "asc", "null_order": "nulls_last"}]}]}""",
                )
            assertThat(altered.status).describedAs(altered.bodyAsText()).isEqualTo(HttpStatusCode.OK)

            // Deferred stats: the file is 'pending' and carries no rows,
            // so there is no range to state — and the row-id span is not
            // substituted, because it is not this table's ordering key.
            val deferred = "s3://b/$catalog/sortedpending/deferred.parquet"
            commitFile(client, "sortedpending", deferred, 100, null)

            // Stats for the OTHER column only: the same absence, reached
            // by a different road (a key column added after the file
            // landed leaves exactly this shape).
            val other = "s3://b/$catalog/sortedpending/other.parquet"
            commitFile(
                client,
                "sortedpending",
                other,
                100,
                """{"field_id": 2, "value_count": 100, "null_count": 0,
                    "lower_bound": "${enc(ColType.STRING, "a")}",
                    "upper_bound": "${enc(ColType.STRING, "b")}"}""",
            )

            val listing = files(client, "sortedpending")
            assertThat(listing.file(deferred).has("ordering_bounds")).isFalse()
            assertThat(listing.file(other).has("ordering_bounds")).isFalse()
        }

    @Test
    fun `an all-null key column's stored null bounds render as JSON null`() =
        api { client ->
            createTable(client, "allnull")
            val altered =
                client.postJson(
                    "$tablesUrl/allnull/alter",
                    """{"ops": [{"op": "set_sort_order", "sort_fields": [
                       {"source_field_id": 1, "direction": "asc", "null_order": "nulls_last"}]}]}""",
                )
            assertThat(altered.status).describedAs(altered.bodyAsText()).isEqualTo(HttpStatusCode.OK)

            val path = "s3://b/$catalog/allnull/f1.parquet"
            commitFile(
                client,
                "allnull",
                path,
                100,
                """{"field_id": 1, "value_count": 100, "null_count": 100}""",
            )

            // A stats row WITH no bounds is a stated answer — the column
            // is all null, so nothing can be pruned on it — and it must
            // not collapse into the "no ordering_bounds" shape, which
            // means the server had nothing to say.
            val file = files(client, "allnull").file(path)
            assertThat(file.has("ordering_bounds")).isTrue()
            assertThat(file["ordering_bounds"]["lower_bound"].isNull).isTrue()
            assertThat(file["ordering_bounds"]["upper_bound"].isNull).isTrue()
        }

    @Test
    fun `the changefeed and the scan plan leave ordering_bounds off`() =
        api { client ->
            createTable(client, "otherreads")
            commitFile(client, "otherreads", "s3://b/$catalog/otherreads/f1.parquet", 100, null)

            // Both embed the same DataFile schema, and both answer a
            // question that is not about one column's span — filling it
            // would tax every consumer poll with a stats join.
            val changes = body(client.get("$tablesUrl/otherreads/changes?from_snapshot=0"))
            assertThat(changes["files"]).isNotEmpty
            assertThat(changes["files"][0].has("ordering_bounds")).isFalse()

            val scan = body(client.get("$tablesUrl/otherreads/scan"))
            assertThat(scan).isNotEmpty
            assertThat(scan[0]["data_file"].has("ordering_bounds")).isFalse()
        }
}
